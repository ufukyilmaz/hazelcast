package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * WAN replication object for cache update operations.
 */
public class WanEnterpriseCacheAddOrUpdateEvent extends WanEnterpriseCacheEvent<CacheEntryView<Object, Object>> {
    private String mergePolicy;
    private WanCacheEntryView<Object, Object> entryView;

    public WanEnterpriseCacheAddOrUpdateEvent(@Nonnull String cacheName,
                                              @Nonnull String mergePolicy,
                                              @Nonnull WanCacheEntryView<Object, Object> entryView,
                                              String managerPrefix, int backupCount) {
        super(cacheName, managerPrefix, backupCount);
        this.mergePolicy = mergePolicy;
        this.entryView = entryView;
    }

    public WanEnterpriseCacheAddOrUpdateEvent() {
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public WanCacheEntryView<Object, Object> getEntryView() {
        return entryView;
    }

    @Nonnull
    @Override
    public Data getKey() {
        return entryView.getDataKey();
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
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementUpdate(getCacheName());
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.ADD_OR_UPDATE;
    }

    @Nullable
    @Override
    public CacheEntryView<Object, Object> getEventObject() {
        return entryView;
    }
}
