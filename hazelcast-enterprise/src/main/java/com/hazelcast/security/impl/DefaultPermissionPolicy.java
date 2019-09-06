package com.hazelcast.security.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.IPermissionPolicy;
import com.hazelcast.security.permission.AllPermissions;
import com.hazelcast.security.permission.AllPermissions.AllPermissionsCollection;
import com.hazelcast.security.permission.ClusterPermission;
import com.hazelcast.security.permission.ClusterPermissionCollection;
import com.hazelcast.security.permission.DenyAllPermissionCollection;

import javax.security.auth.Subject;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.security.SecurityUtil.addressMatches;
import static com.hazelcast.security.SecurityUtil.createPermission;
import static java.lang.Thread.currentThread;

/**
 * The default {@link IPermissionPolicy}.
 *
 * This class is not unused, it's set via {@link com.hazelcast.security.SecurityConstants#DEFAULT_POLICY_CLASS}.
 */
@SuppressWarnings("unused")
public class DefaultPermissionPolicy implements IPermissionPolicy {

    private static final String REGEX_ANY_ENDPOINT = "*.*.*.*";
    private static final String REGEX_ANY_PRINCIPAL = "*";
    private static final ILogger LOGGER = Logger.getLogger(DefaultPermissionPolicy.class.getName());
    private static final PermissionCollection DENY_ALL = new DenyAllPermissionCollection();
    private static final PermissionCollection ALLOW_ALL = new AllPermissionsCollection(true);
    private static final String PRINCIPAL_STRING_SEP = ",";

    private static final Collection<String> ALL_ENDPOINTS = Collections.singleton(REGEX_ANY_ENDPOINT);

    // configured permissions
    final ConcurrentMap<PrincipalKey, PermissionCollection> configPermissions
            = new ConcurrentHashMap<PrincipalKey, PermissionCollection>();

    // principal permissions
    final ConcurrentMap<String, PrincipalPermissionsHolder> principalPermissions
            = new ConcurrentHashMap<String, PrincipalPermissionsHolder>();

    final Object configUpdateMutex = new Object();

    volatile ConfigPatternMatcher configPatternMatcher;

    @Override
    public void configure(Config config, Properties properties) {
        LOGGER.log(Level.FINEST, "Configuring and initializing policy.");
        configPatternMatcher = config.getConfigPatternMatcher();
        loadPermissionConfig(config.getSecurityConfig().getClientPermissionConfigs());
    }

    private void loadPermissionConfig(Set<PermissionConfig> permissionConfigs) {
        for (PermissionConfig permCfg : permissionConfigs) {
            final ClusterPermission permission = createPermission(permCfg);
            // allow all principals
            final String[] principals = permCfg.getPrincipal() != null
                    ? permCfg.getPrincipal().split(PRINCIPAL_STRING_SEP)
                    : new String[] { REGEX_ANY_PRINCIPAL };

            Collection<String> endpoints = permCfg.getEndpoints();
            if (endpoints.isEmpty()) {
                // allow all endpoints
                endpoints = ALL_ENDPOINTS;
            }

            PermissionCollection coll;
            for (final String endpoint : endpoints) {
                for (final String principal : principals) {
                    final PrincipalKey key = new PrincipalKey(principal, endpoint);
                    coll = configPermissions.get(key);
                    if (coll == null) {
                        coll = new ClusterPermissionCollection();
                        configPermissions.put(key, coll);
                    }
                    coll.add(permission);
                }
            }
        }
    }

    /**
     * Returns permission collection of given type for all {@link ClusterRolePrincipal} instances in the given JAAS Subject.
     *
     * @see com.hazelcast.security.IPermissionPolicy#getPermissions(javax.security.auth.Subject, java.lang.Class)
     */
    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public PermissionCollection getPermissions(Subject subject, Class<? extends Permission> type) {
        final Set<ClusterRolePrincipal> principals = subject.getPrincipals(ClusterRolePrincipal.class);
        final Set<ClusterEndpointPrincipal> endpointPrincipals = subject.getPrincipals(ClusterEndpointPrincipal.class);
        if (principals.isEmpty()) {
            return DENY_ALL;
        }
        Iterator<ClusterEndpointPrincipal> endpointIterator = endpointPrincipals.iterator();
        String endpoint = endpointIterator.hasNext() ? endpointIterator.next().getName() : null;
        ClusterPermissionCollection allPrincipalsPermissionCollection = new ClusterPermissionCollection(type);
        for (ClusterRolePrincipal principal : principals) {
            PrincipalPermissionsHolder permissionsHolder;
            do {
                ensurePrincipalPermissions(principal, endpoint);
                String roleEndpointKey = getRoleEndpointKey(principal.getName(), endpoint);
                permissionsHolder = principalPermissions.get(roleEndpointKey);
            } while (permissionsHolder == null);
            if (!permissionsHolder.prepared) {
                try {
                    synchronized (permissionsHolder) {
                        while (!permissionsHolder.prepared) {
                            permissionsHolder.wait();
                        }
                    }
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                    throw new HazelcastException("Interrupted while waiting for the permissions holder to get prepared");
                }
            }

            if (permissionsHolder.hasAllPermissions) {
                return ALLOW_ALL;
            }
            PermissionCollection coll = permissionsHolder.permissions.get(type);
            if (coll == null) {
                coll = DENY_ALL;
                permissionsHolder.permissions.putIfAbsent(type, coll);
            } else if (coll != DENY_ALL) {
                allPrincipalsPermissionCollection.add(coll);
            }
        }
        return allPrincipalsPermissionCollection;
    }

    @Override
    public void refreshPermissions(Set<PermissionConfig> updatedPermissionConfigs) {
        synchronized (configUpdateMutex) {
            configPermissions.clear();
            loadPermissionConfig(updatedPermissionConfigs);
            principalPermissions.clear();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void ensurePrincipalPermissions(ClusterRolePrincipal principal, String endpoint) {
        if (principal == null) {
            return;
        }

        String name = principal.getName();
        String roleEndpointKey = getRoleEndpointKey(name, endpoint);
        if (principalPermissions.containsKey(roleEndpointKey)) {
            return;
        }
        final PrincipalPermissionsHolder permissionsHolder = new PrincipalPermissionsHolder();
        if (principalPermissions.putIfAbsent(roleEndpointKey, permissionsHolder) != null) {
            return;
        }

        try {
            LOGGER.log(Level.FINEST, "Preparing permissions for: " + roleEndpointKey);
            final ClusterPermissionCollection allMatchingPermissionsCollection = new ClusterPermissionCollection();
            synchronized (configUpdateMutex) {
                for (Entry<PrincipalKey, PermissionCollection> e : configPermissions.entrySet()) {
                    final PrincipalKey key = e.getKey();
                    if (nameMatches(name, key.principal) && endpointMatches(endpoint, key.endpoint)) {
                        allMatchingPermissionsCollection.add(e.getValue());
                    }
                }
            }
            final Set<Permission> allMatchingPermissions = allMatchingPermissionsCollection.getPermissions();
            for (Permission perm : allMatchingPermissions) {
                if (perm instanceof AllPermissions) {
                    permissionsHolder.permissions.clear();
                    permissionsHolder.hasAllPermissions = true;
                    LOGGER.log(Level.FINEST, "Granted all-permissions to: " + roleEndpointKey);
                    return;
                }
                Class<? extends Permission> type = perm.getClass();
                ClusterPermissionCollection coll = (ClusterPermissionCollection) permissionsHolder.permissions.get(type);
                if (coll == null) {
                    coll = new ClusterPermissionCollection(type);
                    permissionsHolder.permissions.put(type, coll);
                }
                coll.add(perm);
            }

            LOGGER.log(Level.FINEST, "Compacting permissions for: " + roleEndpointKey);
            final Collection<PermissionCollection> principalCollections = permissionsHolder.permissions.values();
            for (PermissionCollection coll : principalCollections) {
                ((ClusterPermissionCollection) coll).compact();
            }

        } finally {
            synchronized (permissionsHolder) {
                permissionsHolder.prepared = true;
                permissionsHolder.notifyAll();
            }
        }
    }

    private boolean endpointMatches(String endpoint, String pattern) {
        if (endpoint == null) {
            return REGEX_ANY_ENDPOINT.equals(pattern);
        }
        return addressMatches(endpoint, pattern);
    }

    private boolean nameMatches(String name, String pattern) {
        if (name == null) {
            return REGEX_ANY_PRINCIPAL.equals(pattern);
        }
        if (name.equals(pattern)) {
            return true;
        }
        Set<String> patterns = Collections.singleton(pattern);
        return configPatternMatcher.matches(patterns, name) != null;
    }

    private static class PrincipalKey {
        final String principal;
        final String endpoint;

        PrincipalKey(String principal, String endpoint) {
            this.principal = principal;
            this.endpoint = endpoint;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
            result = prime * result + ((principal == null) ? 0 : principal.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final PrincipalKey other = (PrincipalKey) obj;
            return (endpoint != null ? endpoint.equals(other.endpoint) : other.endpoint == null)
                    && (principal != null ? principal.equals(other.principal) : other.principal == null);
        }
    }

    private static class PrincipalPermissionsHolder {
        volatile boolean prepared;
        boolean hasAllPermissions;
        final ConcurrentMap<Class<? extends Permission>, PermissionCollection> permissions =
                new ConcurrentHashMap<Class<? extends Permission>, PermissionCollection>();
    }

    @Override
    public void destroy() {
        principalPermissions.clear();
        configPermissions.clear();
    }

    private static String getRoleEndpointKey(String roleName, String endpoint) {
        return roleName + "@" + (endpoint != null ? endpoint : "");
    }

}