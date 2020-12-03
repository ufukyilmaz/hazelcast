package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.BehmSlotAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Extends {@link BehmSlotAccessor} to support slots pointing to either the record or value.
 */
final class HDIndexBehmSlotAccessor extends BehmSlotAccessor<MemoryBlock> {


    HDIndexBehmSlotAccessor(MemoryAllocator malloc, long baseAddr, long size) {
        super(malloc, baseAddr, size);
    }

    HDIndexBehmSlotAccessor(BehmSlotAccessor that) {
        super(that);
    }

    @Override
    public void setValue(int slot, MemoryBlock v) {
        long value;
        if (v == null) {
            value = 0L;
        } else if (v instanceof NativeMemoryData) {
            value = v.address();
        } else {
            throw new IllegalStateException("Unknown value given");
        }
        setValue(slot, value);
    }
}
