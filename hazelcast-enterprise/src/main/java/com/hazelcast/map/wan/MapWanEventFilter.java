package com.hazelcast.map.wan;


import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.IMap;

/**
 * WAN event filtering interface for {@link IMap}
 * based wan replication events.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface MapWanEventFilter<K, V> {

    /**
     * This method decides whether this entry view is suitable to replicate
     * over WAN.
     *
     * @param mapName the map name
     * @param entryView the entry view
     * @param eventType the WAN event type
     * @return {@code true} if WAN event is not eligible for replication
     */
    boolean filter(String mapName, EntryView<K, V> entryView, WanFilterEventType eventType);
}
