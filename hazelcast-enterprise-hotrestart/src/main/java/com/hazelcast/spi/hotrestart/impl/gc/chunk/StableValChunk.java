package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
public final class StableValChunk extends StableChunk {
    private double benefitToCost;

    public StableValChunk(ActiveValChunk from, boolean compressed) {
        super(from);
    }

    public StableValChunk(long seq, RecordMap records, int liveRecordCount,
                          long size, long garbage, boolean needsDismissing) {
        super(seq, records, liveRecordCount, size, garbage, needsDismissing);
    }

    public long cost() {
        return size() - garbage;
    }

    /** Updates the cached value of the benefit/cost factor.
     * @return the updated value. */
    public double updateBenefitToCost(long currChunkSeq) {
        return benefitToCost = benefitToCost(currChunkSeq);
    }

    @Override public double cachedBenefitToCost() {
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
