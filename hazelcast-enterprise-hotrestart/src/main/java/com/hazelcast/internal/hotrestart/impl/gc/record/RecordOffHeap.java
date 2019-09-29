package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Specialization of {@link Record} as a Flyweight object around a pointer to a native-memory record structure.
 */
public final class RecordOffHeap extends Record {
    public static final int VALUE_RECORD_SIZE = 0x18;
    public static final int TOMBSTONE_SIZE = 0x10;
    private static final int SEQ_OFFSET = 0x0;
    private static final int SIZE_OFFSET = 0x8;
    private static final int ADDITIONAL_INT_OFFSET = 0xc;
    private static final int KEY_PREFIX_OFFSET = 0x10;

    long address = NULL_ADDRESS;

    @Override
    public long rawSeqValue() {
        return AMEM.getLong(address + SEQ_OFFSET);
    }

    @Override
    public int rawSizeValue() {
        return AMEM.getInt(address + SIZE_OFFSET);
    }

    @Override
    public long keyPrefix(KeyHandle ignored) {
        assert !isTombstone() : "Attempt to access key prefix of a tombstone";
        return AMEM.getLong(address + KEY_PREFIX_OFFSET);
    }

    void setKeyPrefix(long prefix) {
        assert !isTombstone() : "Attempt to set key prefix on a tombstone";
        AMEM.putLong(address + KEY_PREFIX_OFFSET, prefix);
    }

    @Override
    public int additionalInt() {
        return AMEM.getInt(address + ADDITIONAL_INT_OFFSET);
    }

    @Override
    public void setAdditionalInt(int value) {
        AMEM.putInt(address + ADDITIONAL_INT_OFFSET, value);
    }

    @Override
    public void negateSeq() {
        AMEM.putLong(address + SEQ_OFFSET, -AMEM.getLong(address + SEQ_OFFSET));
    }

    @Override
    public int decrementGarbageCount() {
        final int newCount = garbageCount() - 1;
        setGarbageCount(newCount);
        return newCount;
    }

    @Override
    public void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    @Override
    public void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        AMEM.putLong(address + SEQ_OFFSET, rawSeqValue);
        AMEM.putInt(address + SIZE_OFFSET, rawSizeValue);
    }
}
