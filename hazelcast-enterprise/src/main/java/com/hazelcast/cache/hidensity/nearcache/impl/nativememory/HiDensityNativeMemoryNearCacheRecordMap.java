package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.impl.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 17/04/15
 */
public class HiDensityNativeMemoryNearCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryNearCacheRecord>
        implements SampleableNearCacheRecordMap<Data, HiDensityNativeMemoryNearCacheRecord> {

    public HiDensityNativeMemoryNearCacheRecordMap(int initialCapacity,
            HiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord> recordProcessor,
            HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
    }

}
