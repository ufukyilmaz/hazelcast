package com.hazelcast.spi.hotrestart.impl.gc;

import java.util.Comparator;

import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
abstract class StableChunk extends Chunk {
    static final Comparator<StableChunk> BY_BENEFIT_COST_DESC = new Comparator<StableChunk>() {
        @Override public int compare(StableChunk left, StableChunk right) {
            final double leftCb = left.cachedBenefitToCost();
            final double rightCb = right.cachedBenefitToCost();
            return leftCb == rightCb ? 0 : leftCb < rightCb ? 1 : -1;
        }
    };

    boolean compressed;
    long size;

    StableChunk(GrowingChunk from, boolean compressed) {
        super(from);
        this.size = from.size();
        this.compressed = compressed;
        this.needsDismissing = from.needsDismissing;
    }

    StableChunk(long seq, RecordMap records, int liveRecordCount,
                long size, long garbage, boolean needsDismissing, boolean compressed
    ) {
        super(seq, records, liveRecordCount, garbage);
        this.size = size;
        this.compressed = compressed;
        this.needsDismissing = needsDismissing;
    }

    @Override final long size() {
        return size;
    }

    @Override final String fnameSuffix() {
        return Chunk.FNAME_SUFFIX + (compressed ? COMPRESSED_SUFFIX : "");
    }

    @Override boolean compressed() {
        return compressed;
    }

    abstract double cachedBenefitToCost();
}
