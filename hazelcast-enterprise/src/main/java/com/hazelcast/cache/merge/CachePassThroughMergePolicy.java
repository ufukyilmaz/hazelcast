package com.hazelcast.cache.merge;

import com.hazelcast.cache.wan.CacheEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * CachePassThroughMerge policy causes the merging entry to be merged from source to destination directly.
 */
public class CachePassThroughMergePolicy implements CacheMergePolicy {


    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        return mergingEntry == null ? existingEntry.getValue() : mergingEntry.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {

    }
}
