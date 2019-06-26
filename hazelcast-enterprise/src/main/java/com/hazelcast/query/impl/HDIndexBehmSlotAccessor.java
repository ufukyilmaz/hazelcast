package com.hazelcast.query.impl;

import com.hazelcast.elastic.map.BehmSlotAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.memory.MemoryBlock;

import static com.hazelcast.query.impl.HDIndexBehmMemoryBlockAccessor.HD_RECORD_BIT_MASK;

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
        } else if (v instanceof HDRecord) {
            value = v.address() | HD_RECORD_BIT_MASK;
        } else {
            throw new IllegalStateException("Unknown value given");
        }
        setValue(slot, value);
    }
}
