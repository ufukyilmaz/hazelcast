package com.hazelcast.internal.serialization.impl;

import java.nio.ByteOrder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

public class EnterpriseOffHeapInputOutputFactory implements InputOutputFactory, OffHeapInputFactory, OffHeapOutputFactory {
    @Override
    public EnterpriseBufferObjectDataInput createInput(Data data, SerializationService service) {
        throw new UnsupportedOperationException("Unsupported for the off-heap de-serialization");
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        throw new UnsupportedOperationException("Unsupported for the off-heap de-serialization");
    }

    @Override
    public EnterpriseBufferObjectDataOutput createOutput(int size, SerializationService service) {
        throw new UnsupportedOperationException("Unsupported for the off-heap serialization");
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public OffHeapDataInput createInput(long dataAddress, long dataSize, SerializationService service) {
        return new EnterpriseOffHeapObjectDataInput(dataAddress, dataSize, service, ByteOrder.BIG_ENDIAN);
    }

    @Override
    public OffHeapDataOutput createOutput(long bufferSize, EnterpriseSerializationService service) {
        return new EnterpriseOffHeapObjectDataOutput(bufferSize, service);
    }
}

