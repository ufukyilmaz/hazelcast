package com.hazelcast.cache.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * WAN replication object for cache remove operations.
 */
public class WanEnterpriseCacheRemoveEvent extends WanEnterpriseCacheEvent<Object> implements SerializationServiceAware {

    private SerializationService serializationService;
    private Data dataKey;
    private transient Object key;

    public WanEnterpriseCacheRemoveEvent(@Nonnull String cacheName,
                                         @Nonnull Data dataKey,
                                         @Nonnull String managerPrefix, int backupCount,
                                         @Nonnull SerializationService serializationService) {
        super(cacheName, managerPrefix, backupCount);
        this.serializationService = serializationService;
        this.dataKey = dataKey;
    }

    public WanEnterpriseCacheRemoveEvent() {
    }

    @Nonnull
    @Override
    public Data getKey() {
        return dataKey;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, dataKey);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        dataKey = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.CACHE_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementRemove(getCacheName());
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.REMOVE;
    }

    @Nullable
    @Override
    public Object getEventObject() {
        if (key == null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
}
