package com.hazelcast.security;

import javax.security.auth.callback.Callback;


/**
 * This {@link Callback} is used to retrieve {@link TokenDeserializer}
 * instance. It is passed to {@link com.hazelcast.security.impl.ClusterCallbackHandler}
 * and used by {@link javax.security.auth.spi.LoginModule}s during login process.
 * <p>
 * <em>Warning!!!</em>
 * We strongly recommend to avoid object deserialization during the authentication
 * to prevent serialization based attacks.
 */
public class TokenDeserializerCallback implements Callback {

    private TokenDeserializer tokenDeserializer;

    public TokenDeserializerCallback() {
    }

    public void setTokenDeserializer(TokenDeserializer tokenDeserializer) {
        this.tokenDeserializer = tokenDeserializer;
    }

    public TokenDeserializer getTokenDeserializer() {
        return tokenDeserializer;
    }
}
