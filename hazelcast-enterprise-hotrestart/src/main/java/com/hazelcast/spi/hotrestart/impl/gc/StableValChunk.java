package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
final class StableValChunk extends StableChunk {
    private double benefitToCost;

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

    /** Updates the cached value of the benefit/cost factor.
     * @return the updated value. */
    double updateBenefitToCost(long currChunkSeq) {
        return benefitToCost = benefitToCost(currChunkSeq);
    }

    @Override double cachedBenefitToCost() {
        return benefitToCost;
    }

    private double benefitToCost(long currChunkSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currChunkSeq - this.seq;
        return age * benefit / cost;
    }
}
