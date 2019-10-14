package com.hazelcast.security.impl;

import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.callback.CallbackHandler;

/**
 * The default {@link ICredentialsFactory} with empty credentials.
 */
public class DefaultCredentialsFactory implements ICredentialsFactory {

    private static final Credentials CREDENTIALS = new UsernamePasswordCredentials(null, null);;

    @Override
    public void configure(CallbackHandler callbackHandler) {
    }

    @Override
    public Credentials newCredentials() {
        return CREDENTIALS;
    }

    @Override
    public void destroy() {
    }
}
