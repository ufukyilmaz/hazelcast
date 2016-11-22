package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * A marker event to initiate WAN sync
 */
public class WanSyncEvent implements DataSerializable {

    /**
     * Marker to indicate if sync event is for all partitions.
     */
    public static final int ALL_PARTITIONS = -99;

    private WanSyncType type;
    private String name;
    private int partitionId = ALL_PARTITIONS;

    private transient WanSyncOperation op;

    public WanSyncEvent() {
    }

    public WanSyncEvent(WanSyncType type) {
        assert type == WanSyncType.ALL_MAPS;
        this.type = type;
    }

    public WanSyncEvent(WanSyncType type, String name) {
        this.type = type;
        this.name = name;
    }

    public WanSyncType getType() {
        return type;
    }

    public void setType(WanSyncType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public WanSyncOperation getOp() {
        return op;
    }

    public void setOp(WanSyncOperation op) {
        this.op = op;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getType());
        out.writeUTF(name);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = WanSyncType.getByType(in.readInt());
        name = in.readUTF();
        partitionId = in.readInt();
    }
}
