package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

/**
 * Flyweight object around a pointer to a native-memory record structure.
 */
public final class RecordOffHeap extends Record {
    public static final int VALUE_RECORD_SIZE = 0x18;
    public static final int TOMBSTONE_SIZE = 0x10;
    private static final int SEQ_OFFSET = 0x0;
    private static final int SIZE_OFFSET = 0x8;
    private static final int ADDITIONAL_INT_OFFSET = 0xc;
    private static final int KEY_PREFIX_OFFSET = 0x10;

    long address = NULL_ADDRESS;

    @Override public long rawSeqValue() {
        return UNSAFE.getLong(address + SEQ_OFFSET);
    }

    @Override public int rawSizeValue() {
        return UNSAFE.getInt(address + SIZE_OFFSET);
    }

    @Override public long keyPrefix(KeyHandle ignored) {
        assert !isTombstone() : "Attempt to access key prefix of a tombstone";
        return UNSAFE.getLong(address + KEY_PREFIX_OFFSET);
    }

    void setKeyPrefix(long prefix) {
        assert !isTombstone() : "Attempt to set key prefix on a tombstone";
        UNSAFE.putLong(address + KEY_PREFIX_OFFSET, prefix);
    }

    @Override public int additionalInt() {
        return UNSAFE.getInt(address + ADDITIONAL_INT_OFFSET);
    }

    @Override public void setAdditionalInt(int value) {
        UNSAFE.putInt(address + ADDITIONAL_INT_OFFSET, value);
    }

    @Override public void negateSeq() {
        UNSAFE.putLong(address + SEQ_OFFSET, -UNSAFE.getLong(address + SEQ_OFFSET));
    }

    @Override public int decrementGarbageCount() {
        final int newCount = garbageCount() - 1;
        setGarbageCount(newCount);
        return newCount;
    }

    @Override public void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    @Override public void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        UNSAFE.putLong(address + SEQ_OFFSET, rawSeqValue);
        UNSAFE.putInt(address + SIZE_OFFSET, rawSizeValue);
    }
}
