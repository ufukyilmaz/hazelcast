package com.hazelcast.hidensity.impl;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be stored
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

        public EvictableSamplingEntry(final int slot) {
            super(slot);
        }

        @Override
        public Object getAccessor() {
            return getKey();
        }

        @Override
        public Evictable getEvictable() {
            return (R) getValue();
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
