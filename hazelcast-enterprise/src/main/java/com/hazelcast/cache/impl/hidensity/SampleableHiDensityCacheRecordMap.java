package com.hazelcast.cache.impl.hidensity;

import com.hazelcast.cache.impl.record.SampleableCacheRecordMap;
import com.hazelcast.internal.hidensity.HiDensityRecordMap;
import com.hazelcast.nio.serialization.Data;

/**
 * @param <R> Type of the cache record stored in this cache record map.
 */
public interface SampleableHiDensityCacheRecordMap<R extends HiDensityCacheRecord>
        extends HiDensityRecordMap<R>, SampleableCacheRecordMap<Data, R> {
}
