package com.hazelcast.security.impl;

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.security.ConfigCallback;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * The default {@link ICredentialsFactory}.
 *
 * This class is not unused, it's set via {@link SecurityConstants#DEFAULT_CREDENTIALS_FACTORY_CLASS}.
 */
public class DefaultCredentialsFactory implements ICredentialsFactory {

    private Credentials credentials;

    @Override
    public void configure(CallbackHandler callbackHandler) {
        ConfigCallback cc = new ConfigCallback();
        try {
            callbackHandler.handle(new Callback[] {cc});
        } catch (IOException | UnsupportedCallbackException e) {
            ExceptionUtil.rethrow(e);
        }
        String clusterName = cc.getConfig().getClusterName();
        credentials = new UsernamePasswordCredentials(clusterName, "");
    }

    @Override
    public Credentials newCredentials() {
        return credentials;
    }

    @Override
    public void destroy() {
    }
}
