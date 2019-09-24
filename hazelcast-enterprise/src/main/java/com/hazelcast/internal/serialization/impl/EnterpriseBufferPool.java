package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolImpl;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * The EnterpriseBufferPool extends the default behavior of the {@link BufferPoolImpl}, but it will not pool
 * BufferObjectDataInput instances.
 */
public class EnterpriseBufferPool extends BufferPoolImpl {

    public EnterpriseBufferPool(InternalSerializationService serializationService) {
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
