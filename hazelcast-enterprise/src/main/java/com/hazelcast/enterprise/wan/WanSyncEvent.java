package com.hazelcast.enterprise.wan;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

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
        assignUuid();
        this.type = type;
    }

    public WanSyncEvent(WanSyncType type, String name) {
        super(name);
        this.type = type;
    }

    private WanSyncEvent(WanSyncType type, UUID uuid, String name) {
        super(uuid, name);
        this.type = type;
    }

    @Override
    public WanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanSyncEvent(type, uuid, mapName);
    }

    public WanSyncType getType() {
        return type;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_SYNC_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(type.getType());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        type = WanSyncType.getByType(in.readInt());
    }

    @Override
    public String toString() {
        return "WanSyncEvent{" + "type=" + type
                + ", mapName='" + mapName + '\''
                + ", partitionSet=" + partitionSet
                + '}';
    }
}
