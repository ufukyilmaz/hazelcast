package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;

/** On-heap specialization of Record */
public class RecordOnHeap extends Record {
    private long seq;
    private int size;
    private int garbageCount;

    public RecordOnHeap(long seq, int size, boolean isTombstone, int garbageCount) {
        this.seq = seq;
        this.size = toRawSizeValue(size, isTombstone);
        this.garbageCount = garbageCount;
    }

    public RecordOnHeap(Record r) {
        setRawSeqSize(r.rawSeqValue(), r.rawSizeValue());
        setGarbageCount(r.garbageCount());
    }

    @Override public final long rawSeqValue() {
        return seq;
    }

    @Override public final int rawSizeValue() {
        return size;
    }

    @Override public long keyPrefix(KeyHandle kh) {
        return ((KeyOnHeap) kh).prefix();
    }

    @Override public final void negateSeq() {
        seq = -seq;
    }

    @Override public final int garbageCount() {
        return garbageCount;
    }

    @Override public final int decrementGarbageCount() {
        return --garbageCount;
    }

    @Override public final void incrementGarbageCount() {
        ++garbageCount;
    }

    @Override public final void setGarbageCount(int newCount) {
        garbageCount = newCount;
    }

    @Override public final void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        this.seq = rawSeqValue;
        this.size = rawSizeValue;
    }
}
