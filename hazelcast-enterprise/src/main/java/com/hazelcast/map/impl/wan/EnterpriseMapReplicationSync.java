package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.io.IOException;

/**
 * WAN replication object for sync requests.
 */
public class EnterpriseMapReplicationSync extends EnterpriseMapReplicationObject {
    private WanMapEntryView<Data, Data> entryView;
    private transient int partitionId;

    public EnterpriseMapReplicationSync(String mapName, EntryView<Data, Data> entryView, int partitionId) {
        super(mapName, 0);
        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<>(entryView);
        }
        this.partitionId = partitionId;
    }

    public EnterpriseMapReplicationSync() {
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
    public int getId() {
        return EWRDataSerializerHook.MAP_REPLICATION_SYNC;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementSync(getMapName());
    }
}
