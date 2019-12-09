package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.WanEventCounters;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * WAN replication object for cache update operations.
 */
public class WanCacheUpdateEvent extends WanCacheEvent {
    private String mergePolicy;
    private WanCacheEntryView entryView;

    public WanCacheUpdateEvent(String cacheName, String mergePolicy,
                               CacheEntryView<Data, Data> entryView,
                               String managerPrefix, int backupCount) {
        super(cacheName, managerPrefix, backupCount);
        this.mergePolicy = mergePolicy;

        if (entryView instanceof WanCacheEntryView) {
            this.entryView = (WanCacheEntryView) entryView;
        } else {
            this.entryView = new WanCacheEntryView(entryView);
        }
    }

    public WanCacheUpdateEvent() {
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public WanCacheEntryView getEntryView() {
        return entryView;
    }

    @Nonnull
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

    @Override
    public int getClassId() {
        return WanDataSerializerHook.CACHE_REPLICATION_UPDATE;
    }

    @Override
    public void incrementEventCount(WanEventCounters counters) {
        counters.incrementUpdate(getCacheName());
    }
}
