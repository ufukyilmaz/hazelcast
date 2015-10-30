package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;

/** On-heap specialization of Record */
class RecordOnHeap extends Record {
    private long seq;
    private int size;
    private int garbageCount;

    RecordOnHeap(long seq, int size, boolean isTombstone, int garbageCount) {
        this.seq = seq;
        this.size = toRawSizeValue(size, isTombstone);
        this.garbageCount = garbageCount;
    }

    RecordOnHeap(Record r) {
        setRawSeqSize(r.rawSeqValue(), r.rawSizeValue());
        setGarbageCount(r.garbageCount());
    }

    @Override final long rawSeqValue() {
        return seq;
    }

    @Override final int rawSizeValue() {
        return size;
    }

    @Override long keyPrefix(KeyHandle kh) {
        return ((KeyOnHeap) kh).prefix();
    }

    @Override final void negateSeq() {
        seq = -seq;
    }

    @Override final int garbageCount() {
        return garbageCount;
    }

    @Override final int decrementGarbageCount() {
        return --garbageCount;
    }

    @Override final void incrementGarbageCount() {
        ++garbageCount;
    }

    @Override final void setGarbageCount(int newCount) {
        garbageCount = newCount;
    }

    @Override final void setRawSeqSize(long rawSeqValue, int rawSizeValue) {
        this.seq = rawSeqValue;
        this.size = rawSizeValue;
    }
}
