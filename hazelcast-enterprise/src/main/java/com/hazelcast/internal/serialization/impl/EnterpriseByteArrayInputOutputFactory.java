package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.nio.ByteOrder;

final class EnterpriseByteArrayInputOutputFactory implements InputOutputFactory {

    private final ByteOrder byteOrder;

    EnterpriseByteArrayInputOutputFactory(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(Data data, InternalSerializationService service) {
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) service;
        return new EnterpriseByteArrayObjectDataInput(data.toByteArray(),
                HeapData.DATA_OFFSET, serializationService, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, InternalSerializationService service) {
        return new EnterpriseByteArrayObjectDataInput(buffer, 0, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, int offset, InternalSerializationService service) {
        return new EnterpriseByteArrayObjectDataInput(buffer, offset, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataOutput createOutput(int size, InternalSerializationService service) {
        return new EnterpriseByteArrayObjectDataOutput(size, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }
}
