package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * On-heap specialization of Tracker.
 */
final class TrackerOnHeap extends Tracker {
    private long chunkSeq;
    private long garbageCount;

    TrackerOnHeap(long chunkSeq, boolean isTombstone) {
        setState(chunkSeq, isTombstone);
    }

    @Override long garbageCount() {
        return garbageCount;
    }

    @Override void setGarbageCount(long garbageCount) {
        this.garbageCount = garbageCount;
    }

    @Override long rawChunkSeq() {
        return chunkSeq;
    }

    @Override void setRawChunkSeq(long rawChunkSeqValue) {
        this.chunkSeq = rawChunkSeqValue;
    }

    @Override public String toString() {
        return "(" + chunkSeq() + ',' + isTombstone() + ',' + garbageCount() + ')';
    }
}
