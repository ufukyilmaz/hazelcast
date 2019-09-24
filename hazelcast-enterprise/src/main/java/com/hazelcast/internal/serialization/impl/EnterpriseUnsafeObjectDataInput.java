package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.EOFException;
import java.io.IOException;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;

final class EnterpriseUnsafeObjectDataInput extends UnsafeObjectDataInput implements EnterpriseBufferObjectDataInput {

    private final EnterpriseSerializationService enterpriseSerializationService;

    EnterpriseUnsafeObjectDataInput(byte[] buffer, int offset, EnterpriseSerializationService service) {
        super(buffer, offset, service);
        this.enterpriseSerializationService = service;
    }

    @Override
    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        if (pos + length > size) {
            throw new EOFException("Size: " + size + ", Position: " + pos + ", Length: " + length);
        }
        if (memory.size() < offset + length) {
            throw new IOException("Cannot write " + length + " bytes to " + memory);
        }
        memory.copyFrom(offset, data, ARRAY_BYTE_BASE_OFFSET + pos, length);
        pos += length;
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return enterpriseSerializationService;
    }

    @Override
    public Data readData(DataType type) throws IOException {
        return EnterpriseSerializationUtil
                .readDataInternal(this, type, enterpriseSerializationService.getCurrentMemoryAllocator(), false);
    }

    @Override
    public Data tryReadData(DataType type) throws IOException {
        return EnterpriseSerializationUtil
                .readDataInternal(this, type, enterpriseSerializationService.getCurrentMemoryAllocator(), true);
    }
}
