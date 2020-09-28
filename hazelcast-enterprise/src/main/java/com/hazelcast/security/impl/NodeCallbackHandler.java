package com.hazelcast.security.impl;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.security.ConfigCallback;
import com.hazelcast.security.HazelcastInstanceCallback;
import com.hazelcast.security.LoggingServiceCallback;
import com.hazelcast.security.RealmConfigCallback;
import com.hazelcast.security.TokenDeserializerCallback;

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
        } else if (cb instanceof RealmConfigCallback) {
            RealmConfigCallback realmCb = (RealmConfigCallback) cb;
            RealmConfig realmCfg = null;
            if (node != null && node.getConfig().getSecurityConfig() != null) {
                realmCfg = node.getConfig().getSecurityConfig().getRealmConfig(realmCb.getRealmName());
            }
            realmCb.setRealmConfig(realmCfg);
        } else if (cb instanceof TokenDeserializerCallback) {
            ((TokenDeserializerCallback) cb).setTokenDeserializer(new TokenDeserializerImpl(node.getSerializationService()));
        } else if (cb instanceof LoggingServiceCallback) {
            ((LoggingServiceCallback) cb).setLoggingService(node.getLoggingService());
        } else if (cb instanceof HazelcastInstanceCallback) {
            ((HazelcastInstanceCallback) cb).setHazelcastInstance(node.hazelcastInstance);
        } else {
            throw new UnsupportedCallbackException(cb);
        }
    }
}
