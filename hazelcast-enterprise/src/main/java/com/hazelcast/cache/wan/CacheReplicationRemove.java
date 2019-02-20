package com.hazelcast.cache.wan;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.io.IOException;

/**
 * WAN replication object for cache remove operations.
 */
public class CacheReplicationRemove extends CacheReplicationObject {

    private Data key;
    private long removeTime;

    public CacheReplicationRemove(String cacheName, Data key, long removeTime, String managerPrefix, int backupCount) {
        super(cacheName, managerPrefix, backupCount);
        this.key = key;
        this.removeTime = removeTime;
    }

    public CacheReplicationRemove() {
    }

    @Override
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

    @Override
    public int getId() {
        return EWRDataSerializerHook.CACHE_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementRemove(getCacheName());
    }
}