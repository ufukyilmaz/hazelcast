package com.hazelcast.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * ClusterCallbackHandler is responsible for handling {@link CredentialsCallback}s.
 */
public class ClusterCallbackHandler implements CallbackHandler {

    private final Credentials credentials;

    public ClusterCallbackHandler(Credentials credentials) {
        super();
        this.credentials = credentials;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof CredentialsCallback) {
                ((CredentialsCallback) cb).setCredentials(credentials);
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }
}
