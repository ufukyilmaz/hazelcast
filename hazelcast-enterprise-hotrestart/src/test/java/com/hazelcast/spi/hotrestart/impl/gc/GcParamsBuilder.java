package com.hazelcast.spi.hotrestart.impl.gc;

import java.lang.reflect.Field;

import static com.hazelcast.spi.hotrestart.impl.gc.GcParams.gcParams;

class GcParamsBuilder {
    private final GcParams gcp = gcParams(1, 1, 1);
    private long costGoal;
    private long maxCost;
    private long currRecordSeq;
    private long reclamationGoal;
    private double minCostBenefit;
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
        setField("currRecordSeq", currRecordSeq);
        setField("reclamationGoal", reclamationGoal);
        setField("minCostBenefit", minCostBenefit);
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

    GcParamsBuilder currRecordSeq(long currRecordSeq) {
        this.currRecordSeq = currRecordSeq;
        return this;
    }

    GcParamsBuilder reclamationGoal(long reclamationGoal) {
        this.reclamationGoal = reclamationGoal;
        return this;
    }

    GcParamsBuilder minCostBenefit(double minCostBenefit) {
        this.minCostBenefit = minCostBenefit;
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
