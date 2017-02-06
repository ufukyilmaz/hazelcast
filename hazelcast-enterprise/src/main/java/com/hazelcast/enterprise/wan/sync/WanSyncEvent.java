package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A marker event to initiate WAN sync for some or all partitions and for all or a specific map.
 */
public class WanSyncEvent implements DataSerializable {
    private WanSyncType type;
    /** The name of the map, can be null in case of {@link WanSyncType#ALL_MAPS} */
    private String name;
    /** The partitions to be synced. If empty, all partitions will be synced */
    private Set<Integer> partitionSet = new HashSet<Integer>();
    /** The operation which should receive the {@link WanSyncResult} */
    private transient WanSyncOperation op;

    @SuppressWarnings("unused")
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

    public String getName() {
        return name;
    }

    public WanSyncOperation getOp() {
        return op;
    }

    public void setOp(WanSyncOperation op) {
        this.op = op;
    }

    public Set<Integer> getPartitionSet() {
        return partitionSet;
    }

    public void setPartitionSet(Set<Integer> partitionSet) {
        this.partitionSet = partitionSet;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getType());
        out.writeUTF(name);
        out.writeInt(partitionSet.size());
        for (Integer partitionId : partitionSet) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = WanSyncType.getByType(in.readInt());
        name = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            partitionSet.add(in.readInt());
        }
    }
}
