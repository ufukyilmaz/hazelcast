package com.hazelcast.spi.hotrestart.impl.gc;

import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.VAL_SIZE_LIMIT_DEFAULT;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Contains GC ergonomics logic: when to GC and how much to GC.
 */
final class GcParams {
    public static final double MIN_GARBAGE_RATIO = 0.05;
    public static final double HIGH_GARBAGE_RATIO = 0.2;
    public static final double MAX_GARBAGE_RATIO = 0.3;
    public static final double BOOSTED_BENEFIT_TO_COST = 5.0;
    public static final double BASE_BENEFIT_TO_COST = 0.4;
    public static final double FORCED_MIN_B2C = 1e-2;
    public static final long MIN_GARBAGE_CHUNKS_TO_FORCE_GC = 10;
    public static final int MAX_COST_CHUNKS = 8;
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_RECORD_COUNT = 1 << 20;
    static final GcParams ZERO = new GcParams(0, 0, 0.0, 0, VAL_SIZE_LIMIT_DEFAULT, false);

    // These drive the chunk selection logic
    final long costGoal;
    final long minCost;
    final long benefitGoal;
    final long maxCost;
    final double minBenefitToCost;
    final boolean forceGc;
    final boolean limitSrcChunks;
    final long currChunkSeq;

    private GcParams(long garbage, long liveData, double ratio, long currChunkSeq, long chunkSize, boolean forceGc) {
        this.currChunkSeq = currChunkSeq;
        this.forceGc = forceGc;
        final long costGoalChunks = max(1, min(MAX_COST_CHUNKS / 2, liveData / chunkSize));
        final long costGoalBytes = chunkSize * costGoalChunks;
        this.limitSrcChunks = costGoalBytes < liveData;
        this.costGoal = limitSrcChunks ? costGoalBytes : liveData;
        this.minCost = ratio < HIGH_GARBAGE_RATIO ? chunkSize / 2 : 0;
        if (forceGc) {
            this.minBenefitToCost = FORCED_MIN_B2C;
            this.benefitGoal = garbageExceedingThreshold(MAX_GARBAGE_RATIO, garbage, liveData);
            this.maxCost = Long.MAX_VALUE;
        } else {
            final double boostBc = BOOSTED_BENEFIT_TO_COST;
            final double baseBc = BASE_BENEFIT_TO_COST;
            final double minRatio = MIN_GARBAGE_RATIO;
            final double highRatio = HIGH_GARBAGE_RATIO;
            this.minBenefitToCost = boostBc - (boostBc - baseBc) * (ratio - minRatio) / (highRatio - minRatio);
            this.benefitGoal = garbageExceedingThreshold(minRatio, garbage, liveData);
            this.maxCost = MAX_COST_CHUNKS * chunkSize;
        }
    }

    static GcParams gcParams(long garbage, long occupancy, long currChunkSeq) {
        final int chunkSize = valChunkSizeLimit();
        final long liveData = occupancy - garbage;
        final double ratio = garbage / (double) liveData;
        final boolean forceGc = ratio >= MAX_GARBAGE_RATIO && garbage >= MIN_GARBAGE_CHUNKS_TO_FORCE_GC * chunkSize;
        return ratio < MIN_GARBAGE_RATIO ? ZERO
                : new GcParams(garbage, liveData, ratio, currChunkSeq, chunkSize, forceGc);
    }

    @Override public String toString() {
        return String.format(
                "(cost goal %,d, min cost %,d, max cost %,d, benefit goal %,d, min benefit/cost %.2f, forceGc %s)",
                costGoal, minCost, maxCost, benefitGoal, minBenefitToCost, forceGc);
    }

    private static long garbageExceedingThreshold(double thresholdRatio, long garbage, long liveData) {
        return 1 + garbage - (long) (thresholdRatio * liveData);
    }
}
