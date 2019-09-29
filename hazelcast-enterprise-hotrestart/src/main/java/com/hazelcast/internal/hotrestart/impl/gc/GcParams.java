package com.hazelcast.internal.hotrestart.impl.gc;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.VAL_SIZE_LIMIT_DEFAULT;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static java.lang.Math.max;
import static java.lang.Math.min;

/** Contains GC ergonomics logic: when to GC and how much to GC. */
final class GcParams {
    public static final double MIN_GARBAGE_RATIO = 0.05;
    public static final double HIGH_GARBAGE_RATIO = 0.2;
    public static final double MAX_PROJECTED_GARBAGE_RATIO = 0.3;
    public static final double BOOSTED_BENEFIT_TO_COST = 5.0;
    public static final double BASE_BENEFIT_TO_COST = 0.4;
    public static final double FORCED_MIN_B2C = 1e-2;
    public static final long MIN_GARBAGE_CHUNKS_TO_FORCE_GC = 10;
    public static final int MAX_COST_CHUNKS = 8;
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_RECORD_COUNT = 1 << 20;
    static final GcParams ZERO = new GcParams(0, 0, 0, 0.0, 0, VAL_SIZE_LIMIT_DEFAULT, false);

    // These drive the chunk selection logic
    /** How many bytes of live data do we have, up to {@link #MAX_COST_CHUNKS}/2 chunks of data */
    final long costGoal;
    /** Min cost in bytes. It is 0.5 chunk size if we have less than {@link #HIGH_GARBAGE_RATIO} garbage, otherwise 0 */
    final long minCost;
    /**
     * Garbage in bytes which exceeds {@link #MIN_GARBAGE_RATIO} for normal or projected garbage in bytes which exceeds
     * {@link #MAX_PROJECTED_GARBAGE_RATIO} for forced GC
     */
    final long benefitGoal;
    /** In normal GC we collect up to {@link #MAX_COST_CHUNKS} chunks. In forced there is no limit. */
    final long maxCost;
    /** Ratio of live to garbage data under which the GC cycle is aborted. In forced GC it is {@link #FORCED_MIN_B2C} */
    final double minBenefitToCost;
    /**
     * GC is forced if the ratio is larger or equal to {@link GcParams#MAX_PROJECTED_GARBAGE_RATIO} and if there is enough
     * garbage to fill {@link GcParams#MIN_GARBAGE_CHUNKS_TO_FORCE_GC} chunks.
     */
    final boolean forceGc;
    /** True if we have more than {@link #MAX_COST_CHUNKS}/2 chunks of live data */
    final boolean limitSrcChunks;
    final long currChunkSeq;

    /**
     * Calculates the GC ergonomics.
     *
     * @param garbage      the amount of garbage in bytes
     * @param occupancy    total size of value chunks in bytes, including live data and garbage
     * @param maxLive      the maximum observed live data in bytes
     * @param ratio        the ratio of garbage to live data
     * @param currChunkSeq the current chunk sequence
     * @param chunkSize    the value chunk size limit (default 8MB)
     * @param forceGc      is GC forced
     */
    private GcParams(long garbage, long occupancy, long maxLive, double ratio,
                     long currChunkSeq, long chunkSize, boolean forceGc
    ) {
        this.currChunkSeq = currChunkSeq;
        this.forceGc = forceGc;
        final long liveData = occupancy - garbage;
        final long costGoalChunks = max(1, min(MAX_COST_CHUNKS / 2, liveData / chunkSize));
        final long costGoalBytes = chunkSize * costGoalChunks;
        this.limitSrcChunks = costGoalBytes < liveData;
        this.costGoal = limitSrcChunks ? costGoalBytes : liveData;
        this.minCost = ratio < HIGH_GARBAGE_RATIO ? chunkSize / 2 : 0;
        if (forceGc) {
            this.minBenefitToCost = FORCED_MIN_B2C;
            this.benefitGoal = garbageExceedingThreshold(MAX_PROJECTED_GARBAGE_RATIO, occupancy - maxLive, maxLive);
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

    /**
     * Calculates the GC params for the given value parameters. If the ratio of garbage to live data is less than
     * {@link GcParams#MIN_GARBAGE_RATIO} then returns zero GC params which signals that there is no need for GC.
     *
     * @param garbage      amount of value garbage in bytes
     * @param occupancy    total size of value chunks in bytes, including live data and garbage
     * @param currChunkSeq the current chunk sequence
     * @return the gc params
     */
    static GcParams gcParams(long garbage, long occupancy, long maxLive, long currChunkSeq) {
        final int chunkSize = valChunkSizeLimit();
        final long liveData = occupancy - garbage;
        return ratio(garbage, liveData) < MIN_GARBAGE_RATIO ? ZERO
                : new GcParams(garbage, occupancy, maxLive, ratio(garbage, liveData),
                currChunkSeq, chunkSize, shouldForceGc(garbage, maxLive, chunkSize));
    }

    private static boolean shouldForceGc(long occupancy, long maxLive, int chunkSize) {
        final long projectedGarbage = occupancy - maxLive;
        return ratio(projectedGarbage, maxLive) >= MAX_PROJECTED_GARBAGE_RATIO
                && projectedGarbage >= MIN_GARBAGE_CHUNKS_TO_FORCE_GC * chunkSize;
    }

    private static double ratio(long garbage, double liveData) {
        return garbage / liveData;
    }

    @Override
    public String toString() {
        return String.format(
                "(cost goal %,d, min cost %,d, max cost %,d, benefit goal %,d, min benefit/cost %.2f, forceGc %s)",
                costGoal, minCost, maxCost, benefitGoal, minBenefitToCost, forceGc);
    }

    private static long garbageExceedingThreshold(double thresholdRatio, long garbage, long liveData) {
        return 1 + garbage - (long) (thresholdRatio * liveData);
    }
}
