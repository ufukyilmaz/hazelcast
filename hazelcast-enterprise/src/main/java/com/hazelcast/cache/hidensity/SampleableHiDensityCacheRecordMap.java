package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.record.SampleableCacheRecordMap;
import com.hazelcast.hidensity.HiDensityRecordMap;
import com.hazelcast.nio.serialization.Data;

/**
 * @param <R> Type of the cache record stored in this cache record map
 *
 * @author sozal 28/11/14
 */
public interface SampleableHiDensityCacheRecordMap<R extends HiDensityCacheRecord>
        extends HiDensityRecordMap<R>, SampleableCacheRecordMap<Data, R> {

}
