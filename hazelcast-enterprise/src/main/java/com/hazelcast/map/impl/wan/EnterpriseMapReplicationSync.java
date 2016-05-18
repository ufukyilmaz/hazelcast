package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * WAN replication object for sync requests.
 */
public class EnterpriseMapReplicationSync extends EnterpriseMapReplicationObject {

    private EntryView<Data, Data> entryView;

    public EnterpriseMapReplicationSync(String mapName, EntryView entryView) {
        super(mapName, 0);
        this.entryView = entryView;
    }

    public EnterpriseMapReplicationSync() {
    }

    public EntryView<Data, Data> getEntryView() {
        return entryView;
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
}
