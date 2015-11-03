package com.hazelcast.wan.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Custom {@link MapMergePolicy} that ignores merging and existing entries and returns null
 * that ends up with deletion of the related key.
 */
public class DeleteMapMergePolicy implements MapMergePolicy {

    @Override
    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
