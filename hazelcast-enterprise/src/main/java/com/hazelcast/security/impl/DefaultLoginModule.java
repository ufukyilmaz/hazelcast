package com.hazelcast.security.impl;

import com.hazelcast.config.Config;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.ConfigCallback;

import java.io.IOException;
import java.util.Arrays;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * The default {@link LoginModule}.
 *
 * This class is not unused, it's set via {@link com.hazelcast.security.SecurityConstants#DEFAULT_LOGIN_MODULE}.
 */
@SuppressWarnings("unused")
public class DefaultLoginModule extends ClusterLoginModule implements LoginModule {

    private String name;

    @Override
    public boolean onLogin() throws LoginException {
        NameCallback ncb = new NameCallback("Name");
        PasswordCallback pcb = new PasswordCallback("Password", false);
        ConfigCallback ccb = new ConfigCallback();

        try {
            callbackHandler.handle(new Callback[] { ncb, pcb, ccb });
        } catch (IOException | UnsupportedCallbackException e) {
            logger.warning("Retrieving the password failed.", e);
            throw new LoginException("Unable to retrieve the password");
        }
        name = ncb.getName();
        char[] pass = pcb.getPassword();
        Config cfg = ccb.getConfig();
        if (cfg == null || cfg.getGroupConfig() == null) {
            throw new LoginException("Group Configuration is not available.");
        }
        String group = cfg.getGroupConfig().getName();
        String expectedPass = cfg.getGroupConfig().getPassword();
        if (group != null && group.equals(name)
                && expectedPass != null && Arrays.equals(pass, expectedPass.toCharArray())) {
            addRole(name);
            return true;
        }
        throw new FailedLoginException("Username/password provided don't match the expected values.");
    }

    @Override
    protected String getName() {
        return name;
    }
}
