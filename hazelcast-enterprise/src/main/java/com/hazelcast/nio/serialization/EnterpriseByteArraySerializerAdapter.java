package com.hazelcast.nio.serialization;


import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.Bits;

import java.io.IOException;

final class EnterpriseByteArraySerializerAdapter extends ByteArraySerializerAdapter
        implements EnterpriseSerializerAdapter {

    EnterpriseByteArraySerializerAdapter(ByteArraySerializer serializer) {
        super(serializer);
    }

    @SuppressWarnings("unchecked")
    public Data write(Object object, MemoryManager memoryManager, int partitionHash) throws IOException {
        byte[] data = serializer.write(object);
        int size = NativeMemoryData.HEADER_LENGTH + data.length;
        if (partitionHash != 0) {
            size += Bits.INT_SIZE_IN_BYTES;
        }
        long address = memoryManager.allocate(size);
        assert address != MemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

        NativeMemoryData binary = new NativeMemoryData(address, size);
        binary.setType(serializer.getTypeId());
        binary.setData(data);
        binary.setPartitionHash(partitionHash);
        return binary;
    }
}
