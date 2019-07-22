package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.SetUtil;

import java.io.IOException;

/**
 * A marker event to initiate WAN sync for map entries.
 */
public class WanSyncEvent extends WanAntiEntropyEvent implements IdentifiedDataSerializable {
    /**
     * WAN sync type. Defines the scope of entries to be synced.
     */
    private WanSyncType type;

    @SuppressWarnings("unused")
    public WanSyncEvent() {
    }

    public WanSyncEvent(WanSyncType type) {
        assert type == WanSyncType.ALL_MAPS;
        this.type = type;
    }

    public WanSyncEvent(WanSyncType type, String name) {
        super(name);
        this.type = type;
    }

    @Override
    public WanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanSyncEvent(type, mapName);
    }

    public WanSyncType getType() {
        return type;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.WAN_SYNC_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getType());
        out.writeUTF(mapName);
        out.writeInt(partitionSet.size());
        for (Integer partitionId : partitionSet) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = WanSyncType.getByType(in.readInt());
        mapName = in.readUTF();
        int size = in.readInt();
        partitionSet = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            partitionSet.add(in.readInt());
        }
    }

    @Override
    public String toString() {
        return "WanSyncEvent{" + "type=" + type
                + ", mapName='" + mapName + '\''
                + ", partitionSet=" + partitionSet
                + '}';
    }
}
