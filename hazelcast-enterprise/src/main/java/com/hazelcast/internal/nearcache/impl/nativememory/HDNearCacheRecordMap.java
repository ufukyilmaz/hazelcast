package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.serialization.Data;

/**
 * {@link SampleableNearCacheRecordMap} implementation for off-heap Near Caches.
 */
public class HDNearCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HDNearCacheRecord>
        implements SampleableNearCacheRecordMap<Data, HDNearCacheRecord> {

    HDNearCacheRecordMap(int initialCapacity,
                         HiDensityRecordProcessor<HDNearCacheRecord> recordProcessor,
                         HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
    }

    private final class NearCacheEvictableSamplingEntry
            extends EvictableSamplingEntry
            implements CacheEntryView {

        private NearCacheEvictableSamplingEntry(int slot) {
            super(slot);
        }

        @Override
        public long getExpirationTime() {
            return getEntryValue().getExpirationTime();
        }

        @Override
        public Object getExpiryPolicy() {
            return null;
        }
    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(final int slot) {
        return new NearCacheEvictableSamplingEntry(slot);
    }
}
