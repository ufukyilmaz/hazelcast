package com.hazelcast.elasticcollections.map;

import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.OffHeapDataUtil;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author mdogan 11/02/14
 */
public class OffHeapDataAccessor
        implements MemoryBlockAccessor<OffHeapData> {

    private final SerializationService serializationService;

    public OffHeapDataAccessor(SerializationService serializationService) {this.serializationService = serializationService;}

    @Override
    public boolean isEqual(long address, OffHeapData value) {
        return OffHeapDataUtil.equals(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return OffHeapDataUtil.equals(address1, address2);
    }

    @Override
    public OffHeapData read(long address) {
        if (address <= 0L) {
            throw new IllegalArgumentException();
        }
        return new OffHeapData().reset(address);
    }

    @Override
    public void dispose(OffHeapData block) {
        serializationService.disposeData(block);
    }

    @Override
    public void dispose(long address) {
        dispose(read(address));
    }
}
