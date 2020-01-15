package com.hazelcast.wan.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;

/**
 * Custom {@link com.hazelcast.spi.merge.SplitBrainMergePolicy}
 * that ignores merging and existing entries and returns
 * null that ends up with deletion of the related key.
 */
class DeleteMapMergePolicy implements SplitBrainMergePolicy<Object, MergingValue<Object>, Object> {

    @Override
    public Object merge(MergingValue<Object> mergingValue, MergingValue<Object> existingValue) {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
