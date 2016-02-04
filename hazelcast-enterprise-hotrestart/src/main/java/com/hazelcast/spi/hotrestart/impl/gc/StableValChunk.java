package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
final class StableValChunk extends StableChunk {
    private double costBenefit;

    StableValChunk(WriteThroughValChunk from, boolean compressed) {
        super(from, compressed);
    }

    StableValChunk(long seq, RecordMap records, int liveRecordCount,
                   long size, long garbage, boolean needsDismissing, boolean compressed) {
        super(seq, records, liveRecordCount, size, garbage, needsDismissing, compressed);
    }

    long cost() {
        return size() - garbage;
    }

    /** Updates the cached value of the cost-benefit factor.
     * @return the updated value. */
    double updateCostBenefit(long currChunkSeq) {
        return costBenefit = costBenefit(currChunkSeq);
    }

    double cachedCostBenefit() {
        return costBenefit;
    }

    private double costBenefit(long currChunkSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currChunkSeq - this.seq;
        return age * benefit / cost;
    }
}
