package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.memory.MemoryAccessor.MEM;

/**
 * Flyweight object around a pointer to a native-memory record structure.
 */
final class RecordOffHeap extends Record {
    public static final int SIZE = 0x18;
    private static final int KEY_PREFIX_OFFSET = 0x0;
    private static final int SEQ_OFFSET = 0x8;
    private static final int SIZE_OFFSET = 0x10;
    private static final int GARBAGE_COUNT_OFFSET = 0x14;

    long address = NULL_ADDRESS;

    @Override long rawSeqValue() {
        return MEM.getLong(address + SEQ_OFFSET);
    }

    @Override int rawSizeValue() {
        return MEM.getInt(address + SIZE_OFFSET);
    }

    @Override long keyPrefix(KeyHandle ignored) {
        return MEM.getLong(address + KEY_PREFIX_OFFSET);
    }

    void setKeyPrefix(long prefix) {
        MEM.putLong(address + KEY_PREFIX_OFFSET, prefix);
    }

    @Override int garbageCount() {
        return MEM.getInt(address + GARBAGE_COUNT_OFFSET);
    }

    @Override void negateSeq() {
        MEM.putLong(address + SEQ_OFFSET, -MEM.getLong(address + SEQ_OFFSET));
    }

    @Override int decrementGarbageCount() {
        final int newCount = garbageCount() - 1;
        setGarbageCount(newCount);
        return newCount;
    }

    @Override void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    @Override public void setGarbageCount(int count) {
        MEM.putInt(address + GARBAGE_COUNT_OFFSET, count);
    }

    @Override void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        MEM.putLong(address + SEQ_OFFSET, rawSeqValue);
        MEM.putInt(address + SIZE_OFFSET, rawSizeValue);
    }
}
