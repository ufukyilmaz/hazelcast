package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolImpl;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * The EnterpriseBufferPool extends the default behavior of the {@link BufferPoolImpl}, but it will not pool
 * BufferObjectDataInput instances.
 */
public class EnterpriseBufferPool extends BufferPoolImpl {

    public EnterpriseBufferPool(SerializationService serializationService) {
        super(serializationService);
    }

    @Override
    public BufferObjectDataInput takeInputBuffer(Data data) {
        return serializationService.createObjectDataInput(data);
    }

    @Override
    public void returnInputBuffer(BufferObjectDataInput in) {
        closeResource(in);
    }
}
