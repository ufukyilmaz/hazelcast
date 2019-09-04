package com.hazelcast.security.impl;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import com.hazelcast.config.Config;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.ConfigCallback;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

/**
 * The default {@link LoginModule}.
 *
 * This class is not unused, it's set via {@link SecurityConstants#DEFAULT_LOGIN_MODULE}.
 */
public class DefaultLoginModule extends ClusterLoginModule implements LoginModule {

    private String name;

    @Override
    public boolean onLogin() throws LoginException {
        CredentialsCallback credcb = new CredentialsCallback();
        ConfigCallback ccb = new ConfigCallback();

        try {
            callbackHandler.handle(new Callback[] { credcb, ccb });
        } catch (IOException | UnsupportedCallbackException e) {
            logger.warning("Retrieving the password failed.", e);
            throw new LoginException("Unable to retrieve the password");
        }
        Credentials credentials = credcb.getCredentials();
        name = credentials.getName();
        Config cfg = ccb.getConfig();
        if (cfg == null) {
            throw new LoginException("Cluster Configuration is not available.");
        }
        UsernamePasswordCredentials upCreds = getCredentialsFromRealm(cfg.getSecurityConfig());
        if (upCreds != null) {
            if (upCreds.equals(credentials)) {
                addRole(name);
                return true;
            }
        } else if (name.equals(cfg.getClusterName())) {
            addRole(name);
            return true;
        }
        throw new FailedLoginException("Username/password provided don't match the expected values.");
    }

    private UsernamePasswordCredentials getCredentialsFromRealm(SecurityConfig securityConfig) {
        String memberRealm = securityConfig.getMemberRealm();
        if (memberRealm == null) {
            logger.warning("Member Realm name is not configured.");
            return null;
        }
        ICredentialsFactory cf = securityConfig.getRealmCredentialsFactory(memberRealm);
        if (cf == null) {
            logger.warning("Member realm name " + memberRealm + " is missing an identity configuration.");
            return null;
        }
        Credentials creds = cf.newCredentials();
        if (! (creds instanceof UsernamePasswordCredentials)) {
            logger.warning("Member realm '" + memberRealm + "' doesn't have username-password identity configured."
                    + " Only cluster-name comparison will be used for authentication.");
            return null;
        }
        return (UsernamePasswordCredentials) creds;
    }

    @Override
    protected String getName() {
        return name;
    }
}
