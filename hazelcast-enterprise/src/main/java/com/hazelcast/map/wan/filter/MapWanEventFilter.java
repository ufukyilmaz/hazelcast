package com.hazelcast.map.wan.filter;


import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;

/**
 * Wan event filtering interface for {@link com.hazelcast.core.IMap}
 * based wan replication events
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface MapWanEventFilter<K, V> {

    /**
     * This method decides whether this entry view is suitable to replicate
     * over WAN
     *
     * @param mapName
     * @param entryView
     * @return <tt>true</tt> if WAN event is not eligible for replication
     */
    boolean filter(String mapName, EntryView<K, V> entryView, WanFilterEventType eventType);
}
