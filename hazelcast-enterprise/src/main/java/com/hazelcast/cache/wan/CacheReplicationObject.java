package com.hazelcast.cache.wan;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Base class for {@link com.hazelcast.cache.ICache} related WAN replication objects
 */
public abstract class CacheReplicationObject implements EnterpriseReplicationEventObject, DataSerializable {

    String cacheName;
    String groupName;
    String uriString;

    public CacheReplicationObject(String cacheName, String groupName, String uriString) {
        this.cacheName = cacheName;
        this.groupName = groupName;
        this.uriString = uriString;
    }

    public CacheReplicationObject() {
    }

    public String getCacheName() {
        return cacheName;
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    public String getUriString() {
        return uriString;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(cacheName);
        out.writeUTF(groupName);
        out.writeUTF(uriString);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readUTF();
        groupName = in.readUTF();
        uriString = in.readUTF();
    }


}
