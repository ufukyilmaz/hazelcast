package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;

/** Specialization of {@link Record} as a plain Java object. */
public class RecordOnHeap extends Record {
    private long seq;
    private int size;
    private int additionalInt;

    public RecordOnHeap(long seq, int size, boolean isTombstone, int additionalInt) {
        this.seq = seq;
        this.size = toRawSizeValue(size, isTombstone);
        this.additionalInt = additionalInt;
    }

    @Override
    public final long rawSeqValue() {
        return seq;
    }

    @Override
    public final int rawSizeValue() {
        return size;
    }

    @Override
    public long keyPrefix(KeyHandle kh) {
        return ((KeyOnHeap) kh).prefix();
    }

    @Override
    public final void negateSeq() {
        seq = -seq;
    }

    @Override
    public final int additionalInt() {
        return additionalInt;
    }

    @Override
    public final int decrementGarbageCount() {
        assert !isTombstone() : "Attempt to decrement garbage count on a tombstone";
        return --additionalInt;
    }

    @Override
    public final void incrementGarbageCount() {
        assert !isTombstone() : "Attempt to increment garbage count on a tombstone";
        ++additionalInt;
    }

    @Override
    public final void setAdditionalInt(int newCount) {
        additionalInt = newCount;
    }

    @Override
    public final void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        this.seq = rawSeqValue;
        this.size = rawSizeValue;
    }
}
