package com.hazelcast.cache.wan;

/**
 * Entry info for WAN replication
 *
 * @param <K> key
 * @param <V> value
 *
 */
public interface CacheEntryView<K, V> {

    /**
     * Key of cache entry
     *
     * @return
     */
    K getKey();

    /**
     * Value of cache entry
     *
     * @return
     */
    V getValue();

    long getExpirationTime();

    long getAccessHit();
}
