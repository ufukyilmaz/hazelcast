package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.SetUtil;

import java.io.IOException;

/**
 * A marker event to compare merkle tree roots for a single map.
 */
public class WanConsistencyCheckEvent extends WanAntiEntropyEvent {

    public WanConsistencyCheckEvent() {
    }

    public WanConsistencyCheckEvent(String mapName) {
        super(mapName);
    }

    @Override
    public WanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanConsistencyCheckEvent(mapName);
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_CONSISTENCY_CHECK_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(partitionSet.size());
        for (Integer partitionId : partitionSet) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        int size = in.readInt();
        partitionSet = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            partitionSet.add(in.readInt());
        }
    }
}
