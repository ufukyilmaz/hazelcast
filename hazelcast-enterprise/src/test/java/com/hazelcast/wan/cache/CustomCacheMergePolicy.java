package com.hazelcast.wan.cache;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;

/**
 * Copy of {@link PassThroughCacheMergePolicy}
 * to test custom merge policy registration
 */
class CustomCacheMergePolicy implements CacheMergePolicy {

    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        return mergingEntry == null ? existingEntry.getValue() : mergingEntry.getValue();
    }
}
