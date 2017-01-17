package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;

public class NativeMapOwnedEntryCostEstimator
        implements OwnedEntryCostEstimator<NativeMemoryData, HDRecord> {

    /**
     * See {@link com.hazelcast.elastic.map.BehmSlotAccessor#SLOT_LENGTH}
     */
    private static final int SLOT_COST_IN_BYTES = 16;

    private volatile long estimate;

    public NativeMapOwnedEntryCostEstimator() {
    }

    @Override
    public long getEstimate() {
        return estimate;
    }

    @Override
    public void adjustEstimateBy(long adjustment) {
        this.estimate += adjustment;
    }

    @Override
    public void reset() {
        estimate = 0L;
    }

    @Override
    public long calculateCost(HDRecord value) {
        return value.size();
    }

    @Override
    public long calculateEntryCost(NativeMemoryData key, HDRecord value) {
        long totalEntryCost = key.size();
        totalEntryCost += value.size();
        totalEntryCost += SLOT_COST_IN_BYTES;
        return totalEntryCost;
    }

}
