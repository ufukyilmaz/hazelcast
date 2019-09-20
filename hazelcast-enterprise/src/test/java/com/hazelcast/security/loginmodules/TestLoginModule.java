package com.hazelcast.security.loginmodules;

import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.internal.util.StringUtil;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

/**
 * Login module for testing behavior of different login phases.
 * <p>
 * Based on configured properties it's able to throw a {@link LoginException} in methods covering the login phases.
 * This login module allows to add set of principals as either {@link ClusterRolePrincipal} instances or just a simple
 * Principal implementations.
 */
public class TestLoginModule implements LoginModule {

    /**
     * Property value which says the LoginModule method (provided as property name) should fail and throw a LoginException.
     */
    public static final String VALUE_ACTION_FAIL = "fail";
    /**
     * Property value which says the LoginModule method (provided as property name) should be ignored (i.e. the method returns
     * {@code false}).
     */
    public static final String VALUE_ACTION_SKIP = "skip";

    /**
     * Login module option name which controls how the {@link #login()} method should result.
     */
    public static final String PROPERTY_RESULT_LOGIN = "login";
    /**
     * Login module option name which controls how the {@link #commit()} method should result.
     */
    public static final String PROPERTY_RESULT_COMMIT = "commit";
    /**
     * Login module option name which controls how the {@link #abort()}method should result.
     */
    public static final String PROPERTY_RESULT_ABORT = "abort";
    /**
     * Login module option name which controls how the {@link #logout()} method should result.
     */
    public static final String PROPERTY_RESULT_LOGOUT = "logout";

    /**
     * Login module option name which holds comma separated list of principal names ({@link SimplePrincipal} instances) to be
     * assigned to JAAS Subject during {@link #commit()}.
     */
    public static final String PROPERTY_PRINCIPALS_SIMPLE = "principals.simple";
    /**
     * Login module option name which holds comma separated list of principal names ({@link ClusterRolePrincipal} instances) to be
     * assigned to JAAS Subject during {@link #commit()}.
     */
    public static final String PROPERTY_PRINCIPALS_ROLE = "principals.role";
    public static final String PROPERTY_PRINCIPALS_IDENTITY = "principals.identity";
    public static final String PROPERTY_PRINCIPALS_ENDPOINT = "principals.endpoint";

    private Map<String, ?> options;
    private Subject subject;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.options = options;
        this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
        return getResultFromOptions(PROPERTY_RESULT_LOGIN);
    }

    @Override
    public boolean commit() throws LoginException {
        boolean result = getResultFromOptions(PROPERTY_RESULT_COMMIT);
        if (result) {
            Set<Principal> principals = subject.getPrincipals();
            for (String name : getPrincipals(PROPERTY_PRINCIPALS_SIMPLE)) {
                principals.add(new SimplePrincipal(name));
            }
            for (String name : getPrincipals(PROPERTY_PRINCIPALS_ROLE)) {
                principals.add(new ClusterRolePrincipal(name));
            }
            for (String name : getPrincipals(PROPERTY_PRINCIPALS_IDENTITY)) {
                principals.add(new ClusterIdentityPrincipal(name));
            }
            for (String name : getPrincipals(PROPERTY_PRINCIPALS_ENDPOINT)) {
                principals.add(new ClusterEndpointPrincipal(name));
            }
        }
        return result;
    }

    @Override
    public boolean abort() throws LoginException {
        return getResultFromOptions(PROPERTY_RESULT_ABORT);
    }

    @Override
    public boolean logout() throws LoginException {
        return getResultFromOptions(PROPERTY_RESULT_LOGOUT);
    }

    private boolean getResultFromOptions(String propertyWithResult) throws LoginException {
        String propertyVal = (String) options.get(propertyWithResult);
        if (propertyVal == null) {
            return true;
        }
        if (VALUE_ACTION_FAIL.equals(propertyVal)) {
            throw new LoginException("Property " + propertyVal + " asked for this exception.");
        }
        return !VALUE_ACTION_SKIP.equals(propertyVal);
    }

    private String[] getPrincipals(String propertyPrincipals) {
        String[] names = StringUtil.splitByComma((String) options.get(propertyPrincipals), false);
        return names == null ? new String[0] : names;
    }

    /**
     * Simple JAAS {@link Principal} implementation, which just holds the name.
     */
    public static class SimplePrincipal implements Principal {

        private final String name;

        public SimplePrincipal(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
