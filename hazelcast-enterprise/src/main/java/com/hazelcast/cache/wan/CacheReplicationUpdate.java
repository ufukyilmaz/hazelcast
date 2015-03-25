package com.hazelcast.cache.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * WAN replication object for cache update operations
 */
public class CacheReplicationUpdate extends CacheReplicationObject {

    String mergePolicy;
    CacheEntryView<Data, Data> entryView;

    public CacheReplicationUpdate(String cacheName, String mergePolicy, CacheEntryView entryView,
                                  String groupName, String uriString) {
        super(cacheName, groupName, uriString);
        this.mergePolicy = mergePolicy;
        this.entryView = entryView;
    }

    public CacheReplicationUpdate() {
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public CacheEntryView<Data, Data> getEntryView() {
        return entryView;
    }

    @Override
    public Data getKey() {
        return entryView.getKey();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(mergePolicy);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        mergePolicy = in.readUTF();
        entryView = in.readObject();
    }
}
