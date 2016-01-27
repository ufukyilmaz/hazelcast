package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
class StableValChunk extends StableChunk {
    private double costBenefit;

    StableValChunk(WriteThroughValChunk from, boolean compressed) {
        super(from, compressed);
    }

    StableValChunk(long seq, RecordMap records, int liveRecordCount,
                   long size, long garbage, boolean needsDismissing, boolean compressed) {
        super(seq, records, liveRecordCount, size, garbage, needsDismissing, compressed);
    }

    final long cost() {
        return size() - garbage;
    }

    final double updateCostBenefit(long currChunkSeq) {
        return costBenefit = costBenefit(currChunkSeq);
    }

    final double costBenefit(long currChunkSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currChunkSeq - this.seq;
        return age * benefit / cost;
    }

    final double cachedCostBenefit() {
        return costBenefit;
    }
}
