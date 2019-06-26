package com.hazelcast.cache.wan;


import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;

/**
 * WAN event filtering interface for cache based WAN replication events.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface CacheWanEventFilter<K, V> {

    /**
     * This method decides whether this entry view is suitable to replicate
     * over WAN.
     *
     * @return {@code true} if WAN event is not eligible for replication.
     */
    boolean filter(String cacheName, CacheEntryView<K, V> entryView, WanFilterEventType eventType);
}
