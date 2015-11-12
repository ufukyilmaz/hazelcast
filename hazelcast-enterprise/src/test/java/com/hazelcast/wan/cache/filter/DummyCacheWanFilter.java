package com.hazelcast.wan.cache.filter;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.wan.filter.CacheWanEventFilter;
import com.hazelcast.enterprise.wan.WanFilterEventType;

public class DummyCacheWanFilter implements CacheWanEventFilter<Integer, String> {

    @Override
    public boolean filter(String cacheName, CacheEntryView<Integer, String> entryView, WanFilterEventType eventType) {
        if (entryView.getKey() == 1 && "A1".equals(entryView.getValue())) {
            return false;
        }
        return true;
    }
}
