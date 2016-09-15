package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.memory.HazelcastMemoryManager;

/**
 * {@link HiDensityNearCacheRecordStore} is the contract point to store keys and values as
 * {@link HiDensityNearCacheRecord} internally and serve them.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @param <R> Type of the hi-density Near Cache record to be stored
 */
public interface HiDensityNearCacheRecordStore<K, V, R extends HiDensityNearCacheRecord>
        extends HiDensityRecordStore<R>, NearCacheRecordStore<K, V> {

    /**
     * Gets underlying {@link com.hazelcast.internal.memory.MemoryManager} implementation.
     *
     * @return the underlying {@link com.hazelcast.internal.memory.MemoryManager} implementation
     */
    HazelcastMemoryManager getMemoryManager();
}
