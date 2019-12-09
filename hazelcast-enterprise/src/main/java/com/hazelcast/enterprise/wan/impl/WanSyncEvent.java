package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.impl.WanSyncType;

import java.io.IOException;
import java.util.UUID;

/**
 * A marker event to initiate WAN sync for map entries.
 */
public class WanSyncEvent extends AbstractWanAntiEntropyEvent implements IdentifiedDataSerializable {
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
    public AbstractWanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanSyncEvent(type, uuid, mapName);
    }

    public WanSyncType getType() {
        return type;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_SYNC_EVENT;
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
