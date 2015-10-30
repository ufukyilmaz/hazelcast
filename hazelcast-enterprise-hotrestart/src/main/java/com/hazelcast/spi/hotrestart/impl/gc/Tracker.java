package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;

/**
 * Tracks the GC state for a single key in the Hot Restart store: where
 * the live record is, whether it's a tombstone, and the number of
 * garbage records for the key.
 */
abstract class Tracker {

    final boolean isAlive() {
        return rawChunkSeq() != 0;
    }

    final long chunkSeq() {
        return isTombstone() ? -rawChunkSeq() : rawChunkSeq();
    }

    final boolean isTombstone() {
        return rawChunkSeq() < 0;
    }

    final void moveToChunk(long destChunkSeq) {
        if (destChunkSeq == 0) {
            throw new HotRestartException("Attempt to move a record to chunk zero");
        }
        setState(destChunkSeq, isTombstone());
    }

    final void newLiveRecord(long chunkSeq, boolean isTombstone) {
        if (isAlive() && !isTombstone()) {
            incrementGarbageCount();
        }
        setState(chunkSeq, isTombstone);
    }

    final void retire() {
        setRawChunkSeq(0);
    }

    final void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    final boolean decrementGarbageCount(int amount) {
        setGarbageCount(garbageCount() - amount);
        assert garbageCount() >= 0 : String.format(
                "Global garbage count went below zero after decrementing by %,d:  %,d", amount, garbageCount());
        return garbageCount() == 0;
    }

    final void resetGarbageCount() {
        setGarbageCount(0);
    }

    void setState(long chunkSeq, boolean isTombstone) {
        setRawChunkSeq(isTombstone ? -chunkSeq : chunkSeq);
    }

    abstract long garbageCount();

    abstract void setGarbageCount(long garbageCount);

    abstract long rawChunkSeq();

    abstract void setRawChunkSeq(long rawChunkSeqValue);
}
