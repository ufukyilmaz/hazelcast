package com.hazelcast.nio.serialization;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.UnsafeHelper;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

class EnterpriseByteArrayObjectDataInput extends ByteArrayObjectDataInput
        implements EnterpriseBufferObjectDataInput {

    private final EnterpriseSerializationService enterpriseSerializationService;

    EnterpriseByteArrayObjectDataInput(byte[] buffer, int offset,
            EnterpriseSerializationService service, ByteOrder byteOrder) {
        super(buffer, offset, service, byteOrder);
        this.enterpriseSerializationService = service;
    }

    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        if (pos + length > size) {
            throw new EOFException("Size: " + size + ", Position: " + pos + ", Length: " + length);
        }
        if (memory.size() < offset + length) {
            throw new IOException("Cannot write " + length + " bytes to " + memory);
        }
        memory.copyFrom(offset, data, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, length);
        pos += length;
    }

    @Override
    public Data readData(DataType type) throws IOException {
        return enterpriseSerializationService.readData(this, type);
    }

    @Override
    public Data tryReadData(DataType type) throws IOException {
        return enterpriseSerializationService.tryReadData(this, type);
    }
}
