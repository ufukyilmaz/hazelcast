package com.hazelcast.map.impl;

import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;

public class NativeMapEntryCostEstimator
        implements EntryCostEstimator<NativeMemoryData, HDRecord> {

    private volatile long additionalCostOfBehmSlots;

    private final HiDensityRecordProcessor recordProcessor;

    public NativeMapEntryCostEstimator(HiDensityRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public long getEstimate() {
        return recordProcessor.getUsedMemory() + additionalCostOfBehmSlots;
    }

    @Override
    public void adjustEstimateBy(long adjustment) {
        additionalCostOfBehmSlots += adjustment;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long calculateValueCost(HDRecord value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long calculateEntryCost(NativeMemoryData key, HDRecord value) {
        throw new UnsupportedOperationException();
    }

}
