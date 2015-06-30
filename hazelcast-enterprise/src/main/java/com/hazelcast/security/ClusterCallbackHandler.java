package com.hazelcast.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

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
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            final Callback cb = callbacks[i];
            if (cb instanceof CredentialsCallback) {
                ((CredentialsCallback) cb).setCredentials(credentials);
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }
}
