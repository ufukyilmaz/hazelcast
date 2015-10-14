package com.hazelcast.wan.cache;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Copy of {@link PassThroughCacheMergePolicy}
 * to test custom merge policy registration
 */
public class CustomCacheMergePolicy implements CacheMergePolicy {

    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        return mergingEntry == null ? existingEntry.getValue() : mergingEntry.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
