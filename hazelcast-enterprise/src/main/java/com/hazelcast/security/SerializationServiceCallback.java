package com.hazelcast.security;

import com.hazelcast.internal.serialization.SerializationService;

import javax.security.auth.callback.Callback;


/**
 * This {@link Callback} is used to retrieve {@link SerializationService} instance. It is passed to
 * {@link ClusterCallbackHandler} and used by {@link javax.security.auth.spi.LoginModule}s during login process.
 * <p>
 * <em>Warning!!!</em> We strongly recommend to avoid object deserialization during the authentication to prevent serialization
 * based attacks.
 */
public class SerializationServiceCallback implements Callback {

    private SerializationService serializationService;

    public SerializationServiceCallback() {
    }

    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }
}
