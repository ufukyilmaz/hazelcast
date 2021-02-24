package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * WAN replication object for map remove operations.
 */
public class WanEnterpriseMapRemoveEvent extends FinalizableWanEnterpriseMapEvent<Object> implements SerializationServiceAware {
    private SerializationService serializationService;
    private Data dataKey;
    private transient Object key;

    public WanEnterpriseMapRemoveEvent(@Nonnull String mapName,
                                       @Nonnull Data dataKey,
                                       int backupCount,
                                       @Nonnull SerializationService serializationService) {
        super(mapName, backupCount);
        this.serializationService = serializationService;
        this.dataKey = dataKey;
    }

    public WanEnterpriseMapRemoveEvent() {
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
        return WanDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementRemove(getMapName());
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
