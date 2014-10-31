package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.nio.serialization.NativeMemoryDataUtil;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author mdogan 11/02/14
 */
class NativeMemoryDataAccessor
        implements MemoryBlockAccessor<NativeMemoryData> {

    private final SerializationService serializationService;

    public NativeMemoryDataAccessor(SerializationService serializationService) {this.serializationService = serializationService;}

    @Override
    public boolean isEqual(long address, NativeMemoryData value) {
        return NativeMemoryDataUtil.equals(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return NativeMemoryDataUtil.equals(address1, address2);
    }

    @Override
    public NativeMemoryData read(long address) {
        if (address <= 0L) {
            throw new IllegalArgumentException();
        }
        return new NativeMemoryData().reset(address);
    }

    @Override
    public void dispose(NativeMemoryData block) {
        serializationService.disposeData(block);
    }

    @Override
    public void dispose(long address) {
        dispose(read(address));
    }
}
