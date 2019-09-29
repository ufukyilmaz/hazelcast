package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;

import java.util.Comparator;

/**
 * Represents a fully constructed chunk whose on-disk contents are stable (immutable). The only kind of
 * update is the retirement of a live record. Not associated with any filesystem resources.
 */
public abstract class StableChunk extends Chunk {
    public static final Comparator<StableChunk> BY_BENEFIT_COST_DESC = new Comparator<StableChunk>() {
        @Override public int compare(StableChunk left, StableChunk right) {
            final double leftCb = left.cachedBenefitToCost();
            final double rightCb = right.cachedBenefitToCost();
            return Double.compare(rightCb, leftCb);
        }
    };

    double benefitToCost;
    private final long size;

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

    @Override
    public final long size() {
        return size;
    }

    /** @return the cached value of the Cost/Benefit factor for this chunk. */
    public final double cachedBenefitToCost() {
        return benefitToCost;
    }
}
