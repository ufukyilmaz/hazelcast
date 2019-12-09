package com.hazelcast.cache.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.WanEventCounters;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * WAN replication object for cache remove operations.
 */
public class WanCacheRemoveEvent extends WanCacheEvent {

    private Data key;

    public WanCacheRemoveEvent(String cacheName, Data key, String managerPrefix, int backupCount) {
        super(cacheName, managerPrefix, backupCount);
        this.key = key;
    }

    public WanCacheRemoveEvent() {
    }

    @Nonnull
    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        key = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.CACHE_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(WanEventCounters counters) {
        counters.incrementRemove(getCacheName());
    }
}
