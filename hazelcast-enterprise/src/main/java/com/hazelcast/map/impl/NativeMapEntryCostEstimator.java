package com.hazelcast.map.impl;

import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.internal.memory.MemoryBlock;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

public class NativeMapEntryCostEstimator
        implements EntryCostEstimator<NativeMemoryData, MemoryBlock> {

    /**
     * See {@link com.hazelcast.internal.elastic.map.BehmSlotAccessor#SLOT_LENGTH}
     */
    private static final int SLOT_COST_IN_BYTES = 16;

    private volatile long estimate;

    private final HiDensityRecordProcessor recordProcessor;

    public NativeMapEntryCostEstimator(HiDensityRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public long getEstimate() {
        return estimate;
    }

    @Override
    public void adjustEstimateBy(long adjustment) {
        estimate += adjustment;
    }

    @Override
    public void reset() {
        estimate = 0;
    }

    @Override
    public long calculateValueCost(MemoryBlock record) {
        if (record instanceof HDRecord) {
            return getSize(record) + getSize((MemoryBlock) ((HDRecord) record).getValue());
        }

        return getSize(record);
    }

    @Override
    public long calculateEntryCost(NativeMemoryData key, MemoryBlock record) {
        if (record instanceof HDRecord) {
            return getSize(record)
                    + getSize((MemoryBlock) ((HDRecord) record).getValue())
                    + getSize(key)
                    + SLOT_COST_IN_BYTES;
        }

        return getSize(key) + getSize(record) + SLOT_COST_IN_BYTES;
    }

    private long getSize(MemoryBlock block) {
        return block != null && block.address() != NULL_ADDRESS ? recordProcessor.getSize(block) : 0L;
    }

}
