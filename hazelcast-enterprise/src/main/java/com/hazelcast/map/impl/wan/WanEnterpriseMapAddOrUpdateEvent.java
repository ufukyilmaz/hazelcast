package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * WAN replication object for map update operations.
 */
public class WanEnterpriseMapAddOrUpdateEvent extends WanEnterpriseMapEvent<EntryView<Object, Object>> {
    /**
     * The policy how to merge the entry on the receiving cluster
     */
    private SplitBrainMergePolicy mergePolicy;
    /**
     * The updated entry
     */
    private WanMapEntryView<Object, Object> entryView;

    public WanEnterpriseMapAddOrUpdateEvent(@Nonnull String mapName,
                                            @Nonnull SplitBrainMergePolicy mergePolicy,
                                            @Nonnull WanMapEntryView<Object, Object> entryView,
                                            int backupCount) {
        super(mapName, backupCount);
        this.mergePolicy = mergePolicy;
        this.entryView = entryView;
    }

    public WanEnterpriseMapAddOrUpdateEvent() {
    }

    public Object getMergePolicy() {
        return mergePolicy;
    }

    public WanMapEntryView<Object, Object> getEntryView() {
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
        out.writeObject(mergePolicy);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        mergePolicy = in.readObject();
        entryView = in.readObject();
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_UPDATE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementUpdate(getMapName());
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.ADD_OR_UPDATE;
    }

    @Nullable
    @Override
    public EntryView<Object, Object> getEventObject() {
        return entryView;
    }
}
