package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;

import java.util.Comparator;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
public abstract class StableChunk extends Chunk {
    public static final Comparator<StableChunk> BY_BENEFIT_COST_DESC = new Comparator<StableChunk>() {
        @Override public int compare(StableChunk left, StableChunk right) {
            final double leftCb = left.cachedBenefitToCost();
            final double rightCb = right.cachedBenefitToCost();
            return Double.compare(rightCb, leftCb);
        }
    };

    public long size;
    double benefitToCost;

    StableChunk(GrowingChunk from) {
        super(from);
        this.size = from.size();
        needsDismissing(from.needsDismissing());
    }

    StableChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage, boolean needsDismissing) {
        super(seq, records, liveRecordCount, garbage);
        this.size = size;
        needsDismissing(needsDismissing);
    }

    @Override public final long size() {
        return size;
    }

    public final double cachedBenefitToCost() {
        return benefitToCost;
    }
}
