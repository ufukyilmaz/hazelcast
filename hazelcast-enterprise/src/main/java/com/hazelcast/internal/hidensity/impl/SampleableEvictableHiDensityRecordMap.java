package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.Data;

/**
 * @param <R> type of the {@link HiDensityRecord} to be stored
 */
public class SampleableEvictableHiDensityRecordMap<R extends HiDensityRecord & Evictable & Expirable>
        extends EvictableHiDensityRecordMap<R>
        implements SampleableEvictableStore<Data, R> {

    public SampleableEvictableHiDensityRecordMap(int initialCapacity,
                                                 HiDensityRecordProcessor<R> recordProcessor,
                                                 HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
    }

    /**
     * Sampling entry that can be evictable and also it is an eviction candidate.
     */
    public class EvictableSamplingEntry extends SamplingEntry implements EvictionCandidate {

        public EvictableSamplingEntry(int slot) {
            super(slot);
        }

        @Override
        public Object getAccessor() {
            return getEntryKey();
        }

        @Override
        public Evictable getEvictable() {
            return (R) getEntryValue();
        }

        @Override
        public Object getKey() {
            return recordProcessor.toObject(getEntryKey());
        }

        @Override
        public Object getValue() {
            return recordProcessor.toObject(((R) getEntryValue()).getValue());
        }

        @Override
        public long getCreationTime() {
            return ((R) getEntryValue()).getCreationTime();
        }

        @Override
        public long getLastAccessTime() {
            return ((R) getEntryValue()).getLastAccessTime();
        }

        @Override
        public long getHits() {
            return ((R) getEntryValue()).getHits();
        }
    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(final int slot) {
        return new EvictableSamplingEntry(slot);
    }

    @Override
    public Iterable<EvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }
}
