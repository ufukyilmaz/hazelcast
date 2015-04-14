package com.hazelcast.cache.wan;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link com.hazelcast.cache.ICache} related WAN replication objects
 */
public abstract class CacheReplicationObject implements EnterpriseReplicationEventObject, DataSerializable {

    String cacheName;
    String uriString;
    Set<String> groupNames = new HashSet<String>();

    public CacheReplicationObject(String cacheName, String uriString) {
        this.cacheName = cacheName;
        this.uriString = uriString;
    }

    public CacheReplicationObject() {
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getUriString() {
        return uriString;
    }

    @Override
    public Set<String> getGroupNames() {
        return groupNames;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(cacheName);
        out.writeUTF(uriString);
        out.writeObject(groupNames);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readUTF();
        uriString = in.readUTF();
        groupNames = in.readObject();
    }
}
