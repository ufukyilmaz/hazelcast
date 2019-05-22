package com.hazelcast.security;

import static java.util.Objects.requireNonNull;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * ClusterCallbackHandler is responsible for handling {@link CredentialsCallback}s.
 */
public class ClusterCallbackHandler implements CallbackHandler {

    private final Credentials credentials;
    private final Connection connection;
    private final Node node;

    public ClusterCallbackHandler(Credentials credentials, Connection connection, Node node) {
        this.credentials = requireNonNull(credentials, "Credentials have to be provided.");
        this.connection = connection;
        this.node = node;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            handleCallback(cb);
        }
    }

    private void handleCallback(Callback cb) throws UnsupportedCallbackException {
        if (cb instanceof NameCallback) {
            ((NameCallback) cb).setName(credentials.getName());
        } else if (cb instanceof PasswordCallback) {
            handlePasswordCallback((PasswordCallback) cb);
        } else if (cb instanceof CredentialsCallback) {
            ((CredentialsCallback) cb).setCredentials(credentials);
        } else if (cb instanceof EndpointCallback) {
            handleEndpointCallback((EndpointCallback) cb);
        } else if (cb instanceof ConfigCallback) {
            ((ConfigCallback) cb).setConfig(node != null ? node.getConfig() : null);
        } else if (cb instanceof SerializationServiceCallback) {
            ((SerializationServiceCallback) cb).setSerializationService(node != null ? node.getSerializationService() : null);
        } else if (cb instanceof CertificatesCallback) {
            ((CertificatesCallback) cb).setCertificates(connection != null ? connection.getRemoteCertificates() : null);
        } else {
            throw new UnsupportedCallbackException(cb);
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
