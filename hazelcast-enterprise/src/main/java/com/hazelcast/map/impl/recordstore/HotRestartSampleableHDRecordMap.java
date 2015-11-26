package com.hazelcast.map.impl.recordstore;

import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.map.impl.record.HDRecord;

/**
 * Hot-restart variant of {@link SampleableElasticHashMap}
 * Provides a tombstone-aware sampling without creating a record for each validity check
 */
public class HotRestartSampleableHDRecordMap extends SampleableElasticHashMap<HDRecord> {

    private final HDRecord samplingRecord;

    public HotRestartSampleableHDRecordMap(int initialCapacity, float loadFactor,
                                           HiDensityRecordProcessor<HDRecord> memoryBlockProcessor) {
        super(initialCapacity, loadFactor, memoryBlockProcessor);
        this.samplingRecord = memoryBlockProcessor.newRecord();
    }

    @Override
    protected boolean isValidForSampling(int slot) {
        if (!accessor.isAssigned(slot)) {
            return false;
        }
        samplingRecord.reset(accessor.getValue(slot));
        return !samplingRecord.isTombstone();
    }
}
