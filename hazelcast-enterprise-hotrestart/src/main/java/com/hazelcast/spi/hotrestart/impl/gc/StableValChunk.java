package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
class StableValChunk extends StableChunk {
    final long youngestRecordSeq;
    private double costBenefit;

    StableValChunk(WriteThroughValChunk from, long youngestRecordSeq, boolean compressed) {
        super(from, compressed);
        this.youngestRecordSeq = youngestRecordSeq;
    }

    StableValChunk(long seq, RecordMap records, int liveRecordCount, long youngestRecordSeq,
                   long size, long garbage, boolean needsDismissing, boolean compressed) {
        super(seq, records, liveRecordCount, size, garbage, needsDismissing, compressed);
        this.youngestRecordSeq = youngestRecordSeq;
    }

    final long cost() {
        return size() - garbage;
    }

    final double updateCostBenefit(long currRecordSeq) {
        return costBenefit = costBenefit(currRecordSeq);
    }

    final double costBenefit(long currRecordSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currRecordSeq - youngestRecordSeq;
        return age * benefit / cost;
    }

    final double cachedCostBenefit() {
        return costBenefit;
    }
}
