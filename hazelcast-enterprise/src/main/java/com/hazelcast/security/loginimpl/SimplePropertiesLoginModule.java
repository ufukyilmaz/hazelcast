package com.hazelcast.security.loginimpl;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.security.ClusterLoginModule;

/**
 * Simple {@link LoginModule} which gets user/password/role configuration directly as login module options.
 */
public class SimplePropertiesLoginModule extends ClusterLoginModule implements LoginModule {

    public static final String OPTION_PREFIX_PASSWORD = "password.";
    public static final String OPTION_PREFIX_ROLES = "roles.";
    public static final String OPTION_ROLE_SEPARATOR = "roleSeparator";
    public static final String DEFAULT_ROLE_SEPARATOR = ",";

    private String name;

    @Override
    public boolean onLogin() throws LoginException {
        NameCallback ncb = new NameCallback("Name");
        PasswordCallback pcb = new PasswordCallback("Password", false);

        try {
            callbackHandler.handle(new Callback[] { ncb, pcb });
        } catch (IOException | UnsupportedCallbackException e) {
            logger.warning("Retrieving the password failed.", e);
            throw new LoginException("Unable to retrieve the password");
        }
        name = ncb.getName();
        char[] pass = pcb.getPassword();

        if (StringUtil.isNullOrEmpty(name) || pass == null || pass.length == 0) {
            throw new FailedLoginException("Empty usernames and/or passwords are not supported");
        }
        String actualPassword = new String(pass);
        String expectedPassword = getStringOption(OPTION_PREFIX_PASSWORD + name, null);
        if (actualPassword.equals(expectedPassword)) {
            for (String role : getStringOption(OPTION_PREFIX_ROLES + name, "").split(DEFAULT_ROLE_SEPARATOR)) {
                addRole(role);
            }
            return true;
        }
        throw new FailedLoginException("Username/password provided don't match the expected values.");
    }

    @Override
    protected String getName() {
        return name;
    }
}
