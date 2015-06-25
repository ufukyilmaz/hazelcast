package com.hazelcast.nio.serialization;

import com.hazelcast.nio.serialization.bufferpool.BufferPool;
import com.hazelcast.nio.serialization.bufferpool.BufferPoolFactory;

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
