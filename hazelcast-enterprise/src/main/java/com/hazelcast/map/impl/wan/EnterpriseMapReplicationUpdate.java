package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * WAN replication object for map update operations.
 */
public class EnterpriseMapReplicationUpdate extends EnterpriseMapReplicationObject {

    private Object mergePolicy;
    private EntryView<Data, Data> entryView;

    public EnterpriseMapReplicationUpdate(String mapName, Object mergePolicy, EntryView<Data, Data> entryView,
                                          int backupCount) {
        super(mapName, backupCount);
        this.mergePolicy = mergePolicy;
        this.entryView = entryView;
    }

    public EnterpriseMapReplicationUpdate() {
    }

    public Object getMergePolicy() {
        return mergePolicy;
    }

    public EntryView<Data, Data> getEntryView() {
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
        entryView = in.readObject();
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.MAP_REPLICATION_UPDATE;
    }
}
