package com.hazelcast.internal.hotrestart.impl.gc;

import java.lang.reflect.Field;

import static com.hazelcast.internal.hotrestart.impl.gc.GcParams.gcParams;

class GcParamsBuilder {

    private final GcParams gcp = gcParams(1, 1, 1, 1);

    private long costGoal;
    private long maxCost;
    private long currChunkSeq;
    private long benefitGoal;
    private double minBenefitToCost;
    private boolean forceGc;
    private boolean limitSrcChunks;

    private GcParamsBuilder() {
    }

    static GcParamsBuilder gcp() {
        return new GcParamsBuilder();
    }

    GcParams build() {
        setField("costGoal", costGoal);
        setField("maxCost", maxCost);
        setField("currChunkSeq", currChunkSeq);
        setField("benefitGoal", benefitGoal);
        setField("minBenefitToCost", minBenefitToCost);
        setField("forceGc", forceGc);
        setField("limitSrcChunks", limitSrcChunks);
        return gcp;
    }

    private void setField(String name, Object value) {
        try {
            final Field f = GcParams.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(gcp, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    GcParamsBuilder costGoal(long costGoal) {
        this.costGoal = costGoal;
        return this;
    }

    GcParamsBuilder maxCost(long maxCost) {
        this.maxCost = maxCost;
        return this;
    }

    GcParamsBuilder currChunkSeq(long currChunkSeq) {
        this.currChunkSeq = currChunkSeq;
        return this;
    }

    GcParamsBuilder benefitGoal(long benefitGoal) {
        this.benefitGoal = benefitGoal;
        return this;
    }

    GcParamsBuilder minBenefitToCost(double minBenefitToCost) {
        this.minBenefitToCost = minBenefitToCost;
        return this;
    }

    GcParamsBuilder forceGc(boolean forceGc) {
        this.forceGc = forceGc;
        return this;
    }

    GcParamsBuilder limitSrcChunks(boolean limitSrcChunks) {
        this.limitSrcChunks = limitSrcChunks;
        return this;
    }
}
