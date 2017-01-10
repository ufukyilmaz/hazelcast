package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Result of {@link WanSyncOperation}
 */
public class WanSyncResult implements DataSerializable {

    private Set<Integer> syncedPartitions = new HashSet<Integer>();

    public WanSyncResult() {
    }

    public Set<Integer> getSyncedPartitions() {
        return syncedPartitions;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(syncedPartitions.size());
        for (Integer syncedPartition : syncedPartitions) {
            out.writeInt(syncedPartition);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            syncedPartitions.add(in.readInt());
        }
    }
}
