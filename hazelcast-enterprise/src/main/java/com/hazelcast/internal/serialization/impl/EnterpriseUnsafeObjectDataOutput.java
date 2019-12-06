package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.io.IOException;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;

final class EnterpriseUnsafeObjectDataOutput extends UnsafeObjectDataOutput
        implements EnterpriseBufferObjectDataOutput {

    EnterpriseUnsafeObjectDataOutput(int size, EnterpriseSerializationService service) {
        super(size, service);
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) service;
    }

    @Override
    public void copyFromMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        ensureAvailable(length);
        if (memory.size() < offset + length) {
            throw new IOException("Cannot read " + length + " bytes from " + memory);
        }
        memory.copyTo(offset, buffer, ARRAY_BYTE_BASE_OFFSET + pos, length);
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
        memory.copyFrom(offset, buffer, ARRAY_BYTE_BASE_OFFSET, length);
    }

    @Override
    public void writeData(Data data) throws IOException {
        if (data instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
            int size =  nativeMemoryData.totalSize();
            writeInt(size);
            if (size > 0) {
                copyFromMemoryBlock(nativeMemoryData, NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD, data.totalSize());
            }
        } else {
            super.writeData(data);
        }
    }
}
