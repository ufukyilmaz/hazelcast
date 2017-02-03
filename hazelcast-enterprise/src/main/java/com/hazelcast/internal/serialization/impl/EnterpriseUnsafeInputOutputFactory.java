package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.nio.ByteOrder;

import static com.hazelcast.internal.serialization.impl.NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;

final class EnterpriseUnsafeInputOutputFactory implements InputOutputFactory {

    @Override
    public EnterpriseBufferObjectDataInput createInput(Data data, InternalSerializationService service) {
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) service;
        if (data instanceof NativeMemoryData) {
            MemoryBlock memoryBlock = (NativeMemoryData) data;
            return new MemoryBlockDataInput(memoryBlock, HeapData.DATA_OFFSET, NATIVE_MEMORY_DATA_OVERHEAD, serializationService);
        } else {
            return new EnterpriseUnsafeObjectDataInput(data.toByteArray(), HeapData.DATA_OFFSET, serializationService);
        }
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, InternalSerializationService service) {
        return new EnterpriseUnsafeObjectDataInput(buffer, 0, (EnterpriseSerializationService) service);
    }

    @Override
    public EnterpriseBufferObjectDataOutput createOutput(int size, InternalSerializationService service) {
        return new EnterpriseUnsafeObjectDataOutput(size, (EnterpriseSerializationService) service);
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
