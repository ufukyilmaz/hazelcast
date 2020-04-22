package com.hazelcast.security;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class ClusterLoginModule implements LoginModule {

    public static final String OPTION_SKIP_IDENTITY = "skipIdentity";
    public static final String OPTION_SKIP_ROLE = "skipRole";
    public static final String OPTION_SKIP_ENDPOINT = "skipEndpoint";

    protected static final String SHARED_STATE_IDENTITY = "hazelcast.last.identity";

    protected final ILogger logger = Logger.getLogger(getClass().getName());

    protected String endpoint;
    protected Subject subject;
    protected Map<String, ?> options;
    protected Map sharedState;
    protected boolean loginSucceeded;
    protected boolean commitSucceeded;
    protected CallbackHandler callbackHandler;

    private Set<String> assignedRoles = new HashSet<>();

    @Override
    public final void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
            Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
        onInitialize();
    }

    @Override
    public final boolean login() throws LoginException {
        if (!getBoolOption(OPTION_SKIP_ENDPOINT, false)) {
            EndpointCallback ecb = new EndpointCallback();
            try {
                callbackHandler.handle(new Callback[] { ecb });
            } catch (Exception e) {
                logger.log(Level.WARNING, "Retrieving the remote address failed", e);
            }
            endpoint = ecb.getEndpoint();
            if (endpoint == null) {
                logger.log(Level.WARNING, "Remote address is empty!");
            }
        }
        if (logger.isFinestEnabled()) {
            logger.log(Level.FINEST, "Authenticating request from " + getEndpointString());
        }
        loginSucceeded = onLogin();
        String name = getName();
        if (loginSucceeded && !isSkipIdentity() && name != null) {
            sharedState.put(SHARED_STATE_IDENTITY, name);
        }
        return loginSucceeded;
    }

    @Override
    public final boolean commit() throws LoginException {
        if (!loginSucceeded) {
            logger.log(Level.WARNING, "Authentication has been failed! Endpoint " + getEndpointString());
            return false;
        }
        String name = getName();
        logger.log(Level.FINEST, "Committing authentication from " + name);
        Set<Principal> principals = subject.getPrincipals();
        if (name != null && !isSkipIdentity()) {
            replaceTypedPrincipal(principals, new ClusterIdentityPrincipal(name));
        }
        if (endpoint != null && !getBoolOption(OPTION_SKIP_ENDPOINT, false)) {
            replaceTypedPrincipal(principals, new ClusterEndpointPrincipal(endpoint));
        }
        if (!isSkipRole()) {
            for (String role : assignedRoles) {
                principals.add(new ClusterRolePrincipal(role));
            }
        }
        commitSucceeded = onCommit();
        return commitSucceeded;
    }

    private void replaceTypedPrincipal(Set<Principal> principals, Principal newPrincipal) {
        Class<? extends Principal> cls = newPrincipal.getClass();
        for (Iterator<Principal> it = principals.iterator(); it.hasNext();) {
            if (cls.isInstance(it.next())) {
                it.remove();
            }
        }
        principals.add(newPrincipal);
    }

    @Override
    public final boolean abort() throws LoginException {
        logger.log(Level.FINEST, "Aborting authentication");
        final boolean abort = onAbort();
        clearSubject();
        loginSucceeded = false;
        commitSucceeded = false;
        return abort;
    }

    @Override
    public final boolean logout() throws LoginException {
        logger.log(Level.FINEST, "Logging out");
        final boolean logout = onLogout();
        clearSubject();
        loginSucceeded = false;
        commitSucceeded = false;
        return logout;
    }

    private void clearSubject() {
        for (Iterator<Principal> it = subject.getPrincipals().iterator(); it.hasNext();) {
            if (it.next() instanceof HazelcastPrincipal) {
                it.remove();
            }
        }
    }

    protected abstract boolean onLogin() throws LoginException;

    protected abstract String getName();

    protected void onInitialize() {
    }

    protected boolean onCommit() throws LoginException {
        return true;
    }

    protected boolean onAbort() throws LoginException {
        return true;
    }

    protected boolean onLogout() throws LoginException {
        return true;
    }

    protected void addRole(String roleName) {
        if (logger.isFineEnabled()) {
            logger.fine("Assigning role: " + roleName);
        }
        assignedRoles.add(roleName);
    }

    protected String getStringOption(String optionName, String defaultValue) {
        String option = getOptionInternal(optionName);
        return option != null ? option.toString() : defaultValue;
    }

    protected boolean getBoolOption(String optionName, boolean defaultValue) {
        String option = getOptionInternal(optionName);
        return option != null ? Boolean.parseBoolean(option) : defaultValue;
    }

    protected int getIntOption(String optionName, int defaultValue) {
        String option = getOptionInternal(optionName);
        return option != null ? Integer.parseInt(option) : defaultValue;
    }

    protected boolean isSkipRole() {
        return getBoolOption(OPTION_SKIP_ROLE, false);
    }

    protected boolean isSkipIdentity() {
        return getBoolOption(OPTION_SKIP_IDENTITY, false);
    }

    protected String getLastIdentity() {
        return (String) sharedState.get(SHARED_STATE_IDENTITY);
    }

    private String getOptionInternal(String optionName) {
        if (options == null) {
            return null;
        }
        Object option = options.get(optionName);
        return option != null ? option.toString() : null;
    }

    private String getEndpointString() {
        return endpoint == null ? "<undefined>" : endpoint;
    }
}
