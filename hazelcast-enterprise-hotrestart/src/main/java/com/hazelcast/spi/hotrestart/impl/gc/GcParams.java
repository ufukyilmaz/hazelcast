package com.hazelcast.spi.hotrestart.impl.gc;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BYTES_PER_RECORD_SEQ_INCREMENT;

/**
 * Contains GC ergonomics logic: when to GC and how much to GC.
 */
final class GcParams {
    public static final double START_COLLECTING_THRESHOLD = 0.1;
    public static final double FORCE_COLLECTING_THRESHOLD = 0.5;
    public static final double NORMAL_MIN_CB = 20 * (Chunk.SIZE_LIMIT / BYTES_PER_RECORD_SEQ_INCREMENT);
    public static final double FORCED_MIN_CB = 1e-2;
    public static final long MIN_GARBAGE_TO_FORCE_GC = 10 * Chunk.SIZE_LIMIT;
    public static final int COST_GOAL_CHUNKS = 2;
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int RECORD_COUNT_LIMIT = 1 << 20;
    static final int SRC_CHUNKS_GOAL = 3;
    static final GcParams ZERO = new GcParams(0, 0, 0.0, 0, false);
    final long costGoal;
    final long currChunkSeq;

    final long reclamationGoal;
    final double minCostBenefit;
    final boolean forceGc;
    final boolean limitSrcChunks;

    private GcParams(long garbage, long liveData, double ratio, long currChunkSeq, boolean forceGc) {
        this.currChunkSeq = currChunkSeq;
        this.forceGc = forceGc;
        final long costGoalBytes = COST_GOAL_CHUNKS * Chunk.SIZE_LIMIT;
        this.limitSrcChunks = costGoalBytes < liveData;
        this.costGoal = limitSrcChunks ? costGoalBytes : liveData;
        if (forceGc) {
            this.minCostBenefit = FORCED_MIN_CB;
            this.reclamationGoal = garbageExceedingThreshold(FORCE_COLLECTING_THRESHOLD, garbage, liveData);
        } else {
            final double excessRatio = (ratio - START_COLLECTING_THRESHOLD)
                    / (FORCE_COLLECTING_THRESHOLD - START_COLLECTING_THRESHOLD);
            this.minCostBenefit = Math.max(FORCED_MIN_CB,
                    NORMAL_MIN_CB - (NORMAL_MIN_CB - FORCED_MIN_CB) * excessRatio);
            this.reclamationGoal = garbageExceedingThreshold(START_COLLECTING_THRESHOLD, garbage, liveData);
        }
    }
    static GcParams gcParams(long garbage, long occupancy, long currChunkSeq) {
        final long liveData = occupancy - garbage;
        final double ratio = garbage / (double) liveData;
        final boolean forceGc =
                ratio >= FORCE_COLLECTING_THRESHOLD && garbage >= MIN_GARBAGE_TO_FORCE_GC;
        return ratio < START_COLLECTING_THRESHOLD ? ZERO
                : new GcParams(garbage, liveData, ratio, currChunkSeq, forceGc);
    }

    @Override public String toString() {
        return "(cost goal " + costGoal
                + ", min cost-benefit " + minCostBenefit
                + ", reclamation goal " + reclamationGoal
                + ", forceGc " + forceGc
                + ')';
    }

    private static long garbageExceedingThreshold(double thresholdRatio, long garbage, long liveData) {
        return 1 + garbage - (long) (thresholdRatio * liveData);
    }

}
