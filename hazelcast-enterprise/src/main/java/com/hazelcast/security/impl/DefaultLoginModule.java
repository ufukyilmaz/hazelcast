package com.hazelcast.security.impl;

import com.hazelcast.config.Config;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.SecurityConstants;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * The default {@link LoginModule}.
 *
 * This class is not unused, it's set via {@link com.hazelcast.security.SecurityConstants#DEFAULT_LOGIN_MODULE}.
 */
@SuppressWarnings("unused")
public class DefaultLoginModule extends ClusterLoginModule implements LoginModule {

    @Override
    public boolean onLogin() throws LoginException {
        if (credentials instanceof UsernamePasswordCredentials) {
            final UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
            final Config cfg = (Config) options.get(SecurityConstants.ATTRIBUTE_CONFIG);
            final String group = cfg.getGroupConfig().getName();
            final String pass = cfg.getGroupConfig().getPassword();

            if (!group.equals(usernamePasswordCredentials.getUsername())) {
                return false;
            }

            if (!pass.equals(usernamePasswordCredentials.getPassword())) {
                return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean onCommit() throws LoginException {
        return loginSucceeded;
    }

    @Override
    protected boolean onAbort() throws LoginException {
        return true;
    }

    @Override
    protected boolean onLogout() throws LoginException {
        return true;
    }
}
