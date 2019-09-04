package com.hazelcast.security.impl;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.security.ConfigCallback;
import com.hazelcast.security.SerializationServiceCallback;

/**
 * NodeCallbackHandler holds a reference to {@link Node} and is able to handle callbacks accessing the Node data (e.g.
 * configuration, serialization service).
 */
public class NodeCallbackHandler implements CallbackHandler {

    private final Node node;

    public NodeCallbackHandler(Node node) {
        this.node = node;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            handleCallback(cb);
        }
    }

    protected void handleCallback(Callback cb) throws UnsupportedCallbackException {
        if (cb instanceof ConfigCallback) {
            ((ConfigCallback) cb).setConfig(node != null ? node.getConfig() : null);
        } else if (cb instanceof SerializationServiceCallback) {
            ((SerializationServiceCallback) cb).setSerializationService(node != null ? node.getSerializationService() : null);
        } else {
            throw new UnsupportedCallbackException(cb);
        }
    }
}
