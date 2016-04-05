package com.hazelcast.spi.hotrestart.impl.gc.tracker;

/**
 * Tracks the GC state for a single key in the Hot Restart store: where
 * the live record is, whether it's a tombstone, and the number of
 * garbage records for the key.
 */
public abstract class Tracker {

    public final boolean isAlive() {
        return rawChunkSeq() != 0;
    }

    public final long chunkSeq() {
        return isTombstone() ? -rawChunkSeq() : rawChunkSeq();
    }

    public final boolean isTombstone() {
        return rawChunkSeq() < 0;
    }

    public final void moveToChunk(long destChunkSeq) {
        setLiveState(destChunkSeq, isTombstone());
    }

    public final void newLiveRecord(long chunkSeq, boolean freshIsTombstone, TrackerMap owner, boolean restarting) {
        final TrackerMapBase ownr = (TrackerMapBase) owner;
        if (isAlive()) {
            final boolean staleIsTombstone = isTombstone();
            if (staleIsTombstone) {
                if (!freshIsTombstone) {
                    ownr.replacedTombstoneWithValue();
                } else {
                    assert restarting : "Attempted to replace a tombstone with another tombstone";
                }
            } else {
                incrementGarbageCount();
                if (freshIsTombstone) {
                    ownr.replacedValueWithTombstone();
                }
            }
        } else {
            ownr.added(freshIsTombstone);
        }
        setLiveState(chunkSeq, freshIsTombstone);
    }

    public final void retire(TrackerMap owner) {
        ((TrackerMapBase) owner).retired(isTombstone());
        setRawChunkSeq(0);
    }

    public final void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    public final boolean decrementGarbageCount(int amount) {
        setGarbageCount(garbageCount() - amount);
        assert garbageCount() >= 0 : String.format(
                "Global garbage count went below zero after decrementing by %,d:  %,d", amount, garbageCount());
        return garbageCount() == 0;
    }

    public final void resetGarbageCount() {
        setGarbageCount(0);
    }

    public final void setLiveState(long chunkSeq, boolean isTombstone) {
        assert chunkSeq != 0 : "Attempt to move a record to chunk zero";
        setRawChunkSeq(isTombstone ? -chunkSeq : chunkSeq);
    }

    public abstract long garbageCount();

    public abstract void setGarbageCount(long garbageCount);

    public abstract long rawChunkSeq();

    public abstract void setRawChunkSeq(long rawChunkSeqValue);
}
