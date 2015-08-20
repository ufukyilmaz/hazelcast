package com.hazelcast.internal.serialization.impl;


import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;

/**
 * The Enterprise implementation of the {@link BufferPoolFactory}.
 *
 * It will create {@link EnterpriseBufferPool} instances.
 */
public class EnterpriseBufferPoolFactory implements BufferPoolFactory {

    @Override
    public BufferPool create(SerializationService serializationService) {
        return new EnterpriseBufferPool(serializationService);
    }
}
