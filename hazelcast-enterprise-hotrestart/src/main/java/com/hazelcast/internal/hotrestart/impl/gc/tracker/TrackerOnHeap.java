package com.hazelcast.internal.hotrestart.impl.gc.tracker;

/**
 * On-heap specialization of Tracker.
 */
final class TrackerOnHeap extends Tracker {
    private long chunkSeq;
    private long garbageCount;

    TrackerOnHeap(long chunkSeq, boolean isTombstone) {
        setLiveState(chunkSeq, isTombstone);
    }

    @Override
    public long garbageCount() {
        return garbageCount;
    }

    @Override
    public void setGarbageCount(long garbageCount) {
        this.garbageCount = garbageCount;
    }

    @Override
    public long rawChunkSeq() {
        return chunkSeq;
    }

    @Override
    public void setRawChunkSeq(long rawChunkSeqValue) {
        this.chunkSeq = rawChunkSeqValue;
    }

    @Override
    public String toString() {
        return "(" + chunkSeq() + ',' + isTombstone() + ',' + garbageCount() + ')';
    }
}
