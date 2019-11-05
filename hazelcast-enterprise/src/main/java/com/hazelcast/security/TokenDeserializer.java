package com.hazelcast.security;

/**
 * Allows to deserialize objects from {@link TokenCredentials} token bytes.
 * <p>
 * <em>Warning!!!</em>
 * <p>
 * We strongly recommend to avoid object deserialization during the authentication to prevent serialization based attacks.
 */
public interface TokenDeserializer {

    /**
     * Deserializes and returns the token value from given {@link TokenCredentials}.
     *
     * @param tokenCredentials {@link TokenCredentials} instance from which the token should be deserialized.
     * @return deserialized token value
     */
    Object deserialize(TokenCredentials tokenCredentials);
}
