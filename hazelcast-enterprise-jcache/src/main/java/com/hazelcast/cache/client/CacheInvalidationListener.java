package com.hazelcast.cache.client;

import com.hazelcast.client.ClientEndpoint;

/**
* @author mdogan 18/02/14
*/
public final class CacheInvalidationListener {

    private final ClientEndpoint endpoint;
    private final int callId;

    public CacheInvalidationListener(ClientEndpoint endpoint, int callId) {
        this.endpoint = endpoint;
        this.callId = callId;
    }

    public void send(CacheInvalidationMessage message) {
        if (endpoint.live()) {
            endpoint.sendEvent(message, callId);
        }
    }
}
