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
    String managerPrefix;
    Set<String> groupNames = new HashSet<String>();

    public CacheReplicationObject(String cacheName, String managerPrefix) {
        this.cacheName = cacheName;
        this.managerPrefix = managerPrefix;
    }

    public CacheReplicationObject() {
    }

    public String getManagerPrefix() {
        return managerPrefix;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getNameWithPrefix() {
        return managerPrefix + cacheName;
    }

    @Override
    public Set<String> getGroupNames() {
        return groupNames;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(cacheName);
        out.writeUTF(managerPrefix);
        out.writeObject(groupNames);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readUTF();
        managerPrefix = in.readUTF();
        groupNames = in.readObject();
    }
}
