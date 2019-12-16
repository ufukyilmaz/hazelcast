package com.hazelcast.security.impl;

import static java.util.Objects.requireNonNull;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.security.TokenDeserializer;

public class TokenDeserializerImpl implements TokenDeserializer {

    private final SerializationService serializationService;

    public TokenDeserializerImpl(SerializationService serializationService) {
        this.serializationService = requireNonNull(serializationService);
    }

    @Override
    public Object deserialize(TokenCredentials tokenCredentials) {
        return tokenCredentials != null ? serializationService.toObject(new HeapData(tokenCredentials.getToken())) : null;
    }
}
