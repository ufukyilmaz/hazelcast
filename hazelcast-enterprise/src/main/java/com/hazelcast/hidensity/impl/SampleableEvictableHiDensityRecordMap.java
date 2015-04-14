package com.hazelcast.hidensity.impl;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.Expirable;
import com.hazelcast.cache.impl.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;

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
                                                 Callback<Data> evictionCallback,
                                                 HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, evictionCallback, storageInfo);
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
