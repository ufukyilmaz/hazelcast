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
        return new EnterpriseByteArrayObjectDataInput(data, (EnterpriseSerializationService) service, byteOrder);
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new EnterpriseByteArrayObjectDataInput(buffer, (EnterpriseSerializationService) service, byteOrder);
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
