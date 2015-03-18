package com.hazelcast.nio.serialization;

import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;

import java.nio.ByteOrder;

final class EnterpriseByteArrayInputOutputFactory implements InputOutputFactory {

    private final ByteOrder byteOrder;

    public EnterpriseByteArrayInputOutputFactory(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(Data data, SerializationService service) {
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) service;
        return new EnterpriseByteArrayObjectDataInput(data.toByteArray(),
                DefaultData.DATA_OFFSET, serializationService, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new EnterpriseByteArrayObjectDataInput(buffer, 0, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new EnterpriseByteArrayObjectDataOutput(size, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }
}
