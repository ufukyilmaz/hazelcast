package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.io.IOException;

/**
 * WAN replication object for map update operations.
 */
public class EnterpriseMapReplicationUpdate extends EnterpriseMapReplicationObject {
    /** The policy how to merge the entry on the receiving cluster */
    private Object mergePolicy;
    /** The updated entry */
    private WanMapEntryView<Data, Data> entryView;

    public EnterpriseMapReplicationUpdate(String mapName, Object mergePolicy,
                                          EntryView<Data, Data> entryView, int backupCount) {
        super(mapName, backupCount);
        this.mergePolicy = mergePolicy;

        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<Data, Data>(entryView);
        }
    }

    public EnterpriseMapReplicationUpdate() {
    }

    public Object getMergePolicy() {
        return mergePolicy;
    }

    public WanMapEntryView<Data, Data> getEntryView() {
        return entryView;
    }

    @Override
    public Data getKey() {
        return entryView.getKey();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(mergePolicy);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        mergePolicy = in.readObject();
        EntryView<Data, Data> entryView = in.readObject();

        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<Data, Data>(entryView);
        }
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.MAP_REPLICATION_UPDATE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementUpdate(getMapName());
    }
}
