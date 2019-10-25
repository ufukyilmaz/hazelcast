package com.hazelcast.security.impl;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.security.impl.SecurityServiceImpl.clonePermissionConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessControlException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.IPermissionPolicy;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecureCallable;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityInterceptor;

public class SecurityContextImpl implements SecurityContext {

    private static final ThreadLocal<ParametersImpl> THREAD_LOCAL_PARAMETERS = new ThreadLocal<>();

    private final Map<String, Map<String, String>> serviceToMethod = new HashMap<String, Map<String, String>>();
    private final ILogger logger;
    private final Node node;
    private final IPermissionPolicy policy;
    private final ICredentialsFactory credentialsFactory;
    private final Configuration memberConfiguration;
    private final Configuration clientConfiguration;
    private final List<SecurityInterceptor> interceptors;
    private final Parameters emptyParameters = new EmptyParametersImpl();
    private final AtomicBoolean refreshPermissionsInProgress = new AtomicBoolean();

    @SuppressWarnings("checkstyle:executablestatementcount")
    public SecurityContextImpl(Node node) {
        super();
        this.node = node;
        logger = node.getLogger("com.hazelcast.enterprise.security");
        logger.log(Level.INFO, "Initializing Hazelcast Enterprise security context.");

        SecurityConfig securityConfig = node.config.getSecurityConfig();

        PermissionPolicyConfig policyConfig = securityConfig.getClientPolicyConfig();
        if (policyConfig.getClassName() == null) {
            policyConfig.setClassName(SecurityConstants.DEFAULT_POLICY_CLASS);
        }
        IPermissionPolicy tmpPolicy = policyConfig.getImplementation();
        if (tmpPolicy == null) {
            tmpPolicy = (IPermissionPolicy) createImplInstance(node.getConfigClassLoader(), policyConfig.getClassName());
        }
        policy = tmpPolicy;
        policy.configure(node.config, policyConfig.getProperties());

        String memberRealm = checkRealmExists(securityConfig.getMemberRealm(), securityConfig, "Member");
        String clientRealm = checkRealmExists(securityConfig.getClientRealm(), securityConfig, "Client");

        ICredentialsFactory credsFact = securityConfig.getRealmCredentialsFactory(memberRealm);
        if (credsFact == null) {
            credsFact = new DefaultCredentialsFactory();
        }
        credentialsFactory = credsFact;
        credentialsFactory.configure(new NodeCallbackHandler(node));

        memberConfiguration = new LoginConfigurationDelegate(securityConfig.getRealmLoginModuleConfigs(memberRealm));
        clientConfiguration = new LoginConfigurationDelegate(securityConfig.getRealmLoginModuleConfigs(clientRealm));
        final List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        interceptors = new ArrayList<>(interceptorConfigs.size());
        for (SecurityInterceptorConfig interceptorConfig : interceptorConfigs) {
            addInterceptors(interceptorConfig);
        }

        fillServiceToMethodMap();

    }

    private String checkRealmExists(String realmName, SecurityConfig securityConfig, String mappingName) {
        if (realmName == null || securityConfig.isRealm(realmName)) {
            return realmName;
        }
        throw new InvalidConfigurationException(
                "Realm name '" + realmName + "' used in " + mappingName + " configuration doesn't exists");
    }

    private void fillServiceToMethodMap() {
        Properties properties = new Properties();
        ClassLoader cl = SecureCallableImpl.class.getClassLoader();
        InputStream stream = cl.getResourceAsStream("permission-mapping.properties");
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(stream);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String action = (String) entry.getValue();
            int dotIndex = key.indexOf('.');
            if (dotIndex == -1) {
                continue;
            }
            String structure = key.substring(0, dotIndex);
            String method = key.substring(dotIndex + 1);
            Map<String, String> methodMap = serviceToMethod.computeIfAbsent(structure, k -> new HashMap<>());
            methodMap.put(method, action);
        }
    }

    public Map<String, Map<String, String>> getServiceToMethod() {
        return serviceToMethod;
    }

    @Override
    public void interceptBefore(Credentials credentials, String objectType, String objectName, String methodName,
                                Object[] args) throws AccessControlException {
        if (interceptors.isEmpty()) {
            return;
        }
        final Parameters parameters = getArguments(args);
        for (SecurityInterceptor interceptor : interceptors) {
            try {
                interceptor.before(credentials, objectType, objectName, methodName, parameters);
            } catch (Throwable t) {
                throw new AccessControlException(t.getMessage());
            }
        }
    }

    @Override
    public void interceptAfter(Credentials credentials, String objectType, String objectName, String methodName) {
        if (interceptors.isEmpty()) {
            return;
        }
        final Parameters parameters = getArguments();
        for (SecurityInterceptor interceptor : interceptors) {
            try {
                interceptor.after(credentials, objectType, objectName, methodName, parameters);
            } catch (Throwable t) {
                logger.warning("Exception during interceptor.after " + interceptor);
            }
        }
    }

    @Override
    public LoginContext createMemberLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException {
        logger.log(Level.FINEST, "Creating Member LoginContext for: " + credentials);
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(SecurityContextImpl.class.getClassLoader());
            String name = node.getConfig().getClusterName();
            ClusterCallbackHandler callbackHandler = new ClusterCallbackHandler(clusterName, credentials, connection, node);
            return new LoginContext(name, new Subject(), callbackHandler, memberConfiguration);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Override
    public LoginContext createClientLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException {
        logger.log(Level.FINEST, "Creating Client LoginContext for: " + credentials);
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            String name = node.getConfig().getClusterName();
            ClusterCallbackHandler callbackHandler = new ClusterCallbackHandler(clusterName, credentials, connection, node);
            return new LoginContext(name, new Subject(), callbackHandler, clientConfiguration);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Override
    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
    }

    @Override
    public void checkPermission(Subject subject, Permission permission) throws SecurityException {
        PermissionCollection coll = policy.getPermissions(subject, permission.getClass());
        final boolean b = coll != null && coll.implies(permission);
        if (!b) {
            throw new AccessControlException("Permission " + permission + " denied!", permission);
        }
    }

    @Override
    public <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable) {
        return new SecureCallableImpl<>(subject, callable, serviceToMethod);
    }

    @Override
    public <V> SecureCallable<?> createSecureCallable(Subject subject, Runnable runnable) {
        return new SecureCallableImpl<V>(subject, runnable, serviceToMethod);
    }

    @Override
    public void destroy() {
        logger.log(Level.INFO, "Destroying Hazelcast Enterprise security context.");
        policy.destroy();
        credentialsFactory.destroy();
    }

    @Override
    public void refreshPermissions(Set<PermissionConfig> permissionConfigs) {
        if (refreshPermissionsInProgress.compareAndSet(false, true)) {
            policy.refreshPermissions(clonePermissionConfigs(permissionConfigs));

            SecurityServiceImpl securityService = (SecurityServiceImpl) node.getSecurityService();
            securityService.setPermissionConfigs(permissionConfigs);
            refreshPermissionsInProgress.set(false);
        } else {
            throw new IllegalStateException("Permissions could not be refreshed, another update is in progress.");
        }
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    private void addInterceptors(SecurityInterceptorConfig interceptorConfig) {
        SecurityInterceptor interceptor = interceptorConfig.getImplementation();
        final String className = interceptorConfig.getClassName();
        if (interceptor == null && className != null) {
            try {
                interceptor = ClassLoaderUtil.newInstance(node.getConfigClassLoader(), className);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (interceptor != null) {
            interceptors.add(interceptor);
        }
    }

    private Parameters getArguments(Object[] args) {
        if (args == null) {
            return emptyParameters;
        }
        ParametersImpl params = THREAD_LOCAL_PARAMETERS.get();
        if (params == null) {
            params = new ParametersImpl(node.getSerializationService());
            THREAD_LOCAL_PARAMETERS.set(params);
        }
        params.setArgs(args);
        return params;
    }

    static Parameters getArguments() {
        return THREAD_LOCAL_PARAMETERS.get();
    }

    private Object createImplInstance(ClassLoader cl, final String className) {
        try {
            return ClassLoaderUtil.newInstance(cl, className);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create instance of '" + className
                    + "', cause: " + e.getMessage(), e);
        }
    }

    private LoginModuleConfig getDefaultLoginModuleConfig() {
        return new LoginModuleConfig(SecurityConstants.DEFAULT_LOGIN_MODULE,
                LoginModuleUsage.REQUIRED);
    }
}
