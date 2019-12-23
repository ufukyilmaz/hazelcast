package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.nearcache.impl.nativememory.HDNearCacheRecord;

/**
 * {@link HDNearCacheRecordStore} is the contract point to store keys and values as
 * {@link HDNearCacheRecord} internally and serve them.
 *
 * @param <K> the type of the key.
 * @param <V> the type of the value.
 * @param <R> Type of the Hi-Density Near Cache record to be stored.
 */
public interface HDNearCacheRecordStore<K, V, R extends HDNearCacheRecord>
        extends HiDensityRecordStore<R>, NearCacheRecordStore<K, V> {

    /**
     * Gets underlying {@link com.hazelcast.internal.memory.MemoryManager} implementation.
     *
     * @return the underlying {@link com.hazelcast.internal.memory.MemoryManager} implementation.
     */
    HazelcastMemoryManager getMemoryManager();
}
