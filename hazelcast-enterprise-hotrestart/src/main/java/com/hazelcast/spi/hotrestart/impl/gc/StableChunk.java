package com.hazelcast.spi.hotrestart.impl.gc;

import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
class StableChunk extends Chunk {
    final long youngestRecordSeq;
    boolean compressed;
    private final long size;
    private double costBenefit;

    StableChunk(WriteThroughChunk from, long youngestRecordSeq, boolean compressed) {
        super(from);
        this.size = from.size();
        this.youngestRecordSeq = youngestRecordSeq;
        this.compressed = compressed;
        this.needsDismissing = from.needsDismissing;
    }

    StableChunk(long seq, RecordMap records, int liveRecordCount, long youngestRecordSeq,
                long size, long garbage, boolean needsDismissing, boolean compressed) {
        super(seq, records, liveRecordCount, garbage);
        this.size = size;
        this.youngestRecordSeq = youngestRecordSeq;
        this.compressed = compressed;
        this.needsDismissing = needsDismissing;
    }

    @Override final long size() {
        return size;
    }

    final long cost() {
        return size() - garbage;
    }

    final double updateCostBenefit(long currSeq) {
        return costBenefit = costBenefit(currSeq);
    }

    final double costBenefit(long currSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currSeq - youngestRecordSeq;
        return age * benefit / cost;
    }

    final double cachedCostBenefit() {
        return costBenefit;
    }

    @Override final String fnameSuffix() {
        return Chunk.FNAME_SUFFIX + (compressed ? COMPRESSED_SUFFIX : "");
    }
}
