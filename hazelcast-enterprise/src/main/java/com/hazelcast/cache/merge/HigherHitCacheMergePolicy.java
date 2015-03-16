package com.hazelcast.cache.merge;

import com.hazelcast.cache.wan.CacheEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * HigherHitsCacheMergePolicy causes the merging entry to be merged from source to destination cache
 * if source entry has more hits than the destination one.
 */
public class HigherHitCacheMergePolicy implements CacheMergePolicy {

    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        if (mergingEntry.getAccessHit() >= existingEntry.getAccessHit()) {
            return mergingEntry.getValue();
        }
        return existingEntry.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
