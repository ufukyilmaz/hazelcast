package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains the results of WAN synchronization operations.
 *
 * @see MemberMapSyncOperation
 * @see PartitionMapSyncOperation
 */
public class SyncResult implements DataSerializable {

    private Set<Integer> partitionIds = new HashSet<Integer>();

    public Set<Integer> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            partitionIds.add(in.readInt());
        }
    }
}
