package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.DistributedServiceWanEventCounters;

import java.io.IOException;
import java.util.UUID;

/**
 * WAN replication object for sync requests.
 */
public class EnterpriseMapReplicationSync extends EnterpriseMapReplicationObject {
    private transient UUID uuid;
    private WanMapEntryView<Data, Data> entryView;
    private transient int partitionId;

    public EnterpriseMapReplicationSync(UUID uuid, String mapName, EntryView<Data, Data> entryView, int partitionId) {
        super(mapName, 0);
        this.uuid = uuid;
        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<>(entryView);
        }
        this.partitionId = partitionId;
    }

    public EnterpriseMapReplicationSync() {
    }

    public UUID getUuid() {
        return uuid;
    }

    public WanMapEntryView<Data, Data> getEntryView() {
        return entryView;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        entryView = in.readObject();
    }

    @Override
    public Data getKey() {
        return entryView.getKey();
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.MAP_REPLICATION_SYNC;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementSync(getMapName());
    }
}
