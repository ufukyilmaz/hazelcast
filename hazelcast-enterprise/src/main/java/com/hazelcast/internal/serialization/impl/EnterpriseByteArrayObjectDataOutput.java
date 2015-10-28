package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.IOException;
import java.nio.ByteOrder;

class EnterpriseByteArrayObjectDataOutput extends ByteArrayObjectDataOutput implements EnterpriseBufferObjectDataOutput {

    EnterpriseByteArrayObjectDataOutput(int size, EnterpriseSerializationService service, ByteOrder byteOrder) {
        super(size, service, byteOrder);
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) service;
    }

    public void copyFromMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        ensureAvailable(length);
        if (memory.size() < offset + length) {
            throw new IOException("Cannot read " + length + " bytes from " + memory);
        }
        memory.copyTo(offset, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, length);
        pos += length;
    }

    @Override
    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        if (pos < length) {
            throw new IOException("Not enough data available!");
        }
        if (memory.size() < offset + length) {
            throw new IOException("Cannot write " + length + " bytes to " + memory);
        }
        memory.copyFrom(offset, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, length);
    }

    @Override
    public void writeData(Data data) throws IOException {
        if (data instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
            int size =  nativeMemoryData.totalSize();
            writeInt(size);
            if (size > 0) {
                copyFromMemoryBlock(nativeMemoryData, NativeMemoryData.COPY_OFFSET, data.totalSize());
            }
        } else {
            super.writeData(data);
        }
    }
}
