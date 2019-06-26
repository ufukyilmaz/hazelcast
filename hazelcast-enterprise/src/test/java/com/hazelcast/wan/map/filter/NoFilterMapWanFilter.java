package com.hazelcast.wan.map.filter;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.wan.MapWanEventFilter;

/**
 * Used for testing purposes.
 *
 * This filter does not do any filtering. Passes whatever comes.
 */
public class NoFilterMapWanFilter implements MapWanEventFilter<Integer, String> {

    @Override
    public boolean filter(String mapName,
                          EntryView<Integer, String> entryView, WanFilterEventType eventType) {
        return false;
    }
}
