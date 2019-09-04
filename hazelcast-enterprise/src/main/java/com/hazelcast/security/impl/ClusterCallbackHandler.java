package com.hazelcast.security.impl;

import static java.util.Objects.requireNonNull;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.CertificatesCallback;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.EndpointCallback;
import com.hazelcast.security.PasswordCredentials;

/**
 * ClusterCallbackHandler is responsible for handling JAAS callbacks in Hazelcast login modules.
 */
public class ClusterCallbackHandler extends NodeCallbackHandler {

    private final Credentials credentials;
    private final Connection connection;

    public ClusterCallbackHandler(Credentials credentials, Connection connection, Node node) {
        super(node);
        this.credentials = requireNonNull(credentials, "Credentials have to be provided.");
        this.connection = connection;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            handleCallback(cb);
        }
    }

    @Override
    protected void handleCallback(Callback cb) throws UnsupportedCallbackException {
        if (cb instanceof NameCallback) {
            ((NameCallback) cb).setName(credentials.getName());
        } else if (cb instanceof PasswordCallback) {
            handlePasswordCallback((PasswordCallback) cb);
        } else if (cb instanceof CredentialsCallback) {
            ((CredentialsCallback) cb).setCredentials(credentials);
        } else if (cb instanceof EndpointCallback) {
            handleEndpointCallback((EndpointCallback) cb);
        } else if (cb instanceof CertificatesCallback) {
            ((CertificatesCallback) cb).setCertificates(connection != null ? connection.getRemoteCertificates() : null);
        } else {
            super.handleCallback(cb);
        }
    }

    private void handleEndpointCallback(EndpointCallback cb) {
        if (connection != null) {
            cb.setEndpoint(connection.getInetAddress().getHostAddress());
        }
    }

    private void handlePasswordCallback(PasswordCallback cb) {
        char[] password = null;
        if (credentials instanceof PasswordCredentials) {
            PasswordCredentials passwordCredentials = (PasswordCredentials) credentials;
            String pwdStr = passwordCredentials.getPassword();
            password = pwdStr == null ? null : pwdStr.toCharArray();
        }
        cb.setPassword(password);
    }
}
