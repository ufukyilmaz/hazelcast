package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.hidensity.HiDensityRecordStore;

/**
 * {@link HiDensityNearCacheRecordStore} is the contract point to store keys and values as
 * {@link HiDensityNearCacheRecord} internally and serve them.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @param <R> Type of the hi-density near-cache record to be stored
 *
 * @author sozal 26/10/14
 */
public interface HiDensityNearCacheRecordStore<K, V, R extends HiDensityNearCacheRecord>
        extends HiDensityRecordStore<R>, NearCacheRecordStore<K, V> {

}
