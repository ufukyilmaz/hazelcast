package com.hazelcast.elastic.map;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;

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
    public long dispose(NativeMemoryData block) {
        long size = block.size();
        serializationService.disposeData(block);
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }
}
