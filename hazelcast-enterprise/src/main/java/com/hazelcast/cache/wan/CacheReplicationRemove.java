package com.hazelcast.cache.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * WAN replication object for cache remove operations
 */
public class CacheReplicationRemove extends CacheReplicationObject {

    Data key;
    long removeTime;

    public CacheReplicationRemove(String cacheName, Data key, long removeTime, String managerPrefix) {
        super(cacheName, managerPrefix);
        this.key = key;
        this.removeTime = removeTime;
    }

    public CacheReplicationRemove() {
    }

    public Data getKey() {
        return key;
    }

    public long getRemoveTime() {
        return removeTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(removeTime);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        removeTime = in.readLong();
        key = in.readData();
    }
}
