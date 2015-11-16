package com.hazelcast.wan.map.filter;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.wan.filter.MapWanEventFilter;

public class DummyMapWanFilter implements MapWanEventFilter<Integer, String> {

    @Override
    public boolean filter(String mapName, EntryView<Integer, String> entryView, WanFilterEventType eventType) {
        if (entryView.getKey() == 1 && "A1".equals(entryView.getValue())) {
            return false;
        }
        return true;
    }
}
