package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;

/**
 * WAN replication object for sync requests.
 */
public class WanEnterpriseMapSyncEvent extends WanEnterpriseMapEvent<EntryView<Object, Object>> {
    private transient UUID uuid;
    private WanMapEntryView<Object, Object> entryView;
    private transient int partitionId;

    public WanEnterpriseMapSyncEvent(UUID uuid, String mapName, WanMapEntryView<Object, Object> entryView,
                                     int partitionId) {
        super(mapName, 0);
        this.uuid = uuid;
        this.entryView = entryView;
        this.partitionId = partitionId;
    }

    public WanEnterpriseMapSyncEvent() {
    }

    public UUID getUuid() {
        return uuid;
    }

    public WanMapEntryView<Object, Object> getEntryView() {
        return entryView;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        entryView = in.readObject();
    }

    @Nonnull
    @Override
    public Data getKey() {
        return entryView.getDataKey();
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_SYNC;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementSync(getMapName());
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.SYNC;
    }

    @Nullable
    @Override
    public EntryView<Object, Object> getEventObject() {
        return entryView;
    }
}
