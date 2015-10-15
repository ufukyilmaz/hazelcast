package com.hazelcast.cache.wan.filter;


import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;

/**
 * Wan event filtering interface for cache based wan replication events
 */
public interface CacheWanEventFilter {

    /**
     * This method decides whether this entry view is suitable to replicate
     * over WAN.
     *
     * @param entryView
     * @return <tt>true</tt> if WAN event is not eligible for replication.
     */
    boolean filter(String cacheName, CacheEntryView entryView, WanFilterEventType eventType);
}
