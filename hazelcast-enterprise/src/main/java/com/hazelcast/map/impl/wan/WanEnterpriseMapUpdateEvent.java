package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.WanEventCounters;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * WAN replication object for map update operations.
 */
public class WanEnterpriseMapUpdateEvent extends WanEnterpriseMapEvent {
    /**
     * The policy how to merge the entry on the receiving cluster
     */
    private Object mergePolicy;
    /**
     * The updated entry
     */
    private WanMapEntryView<Data, Data> entryView;

    public WanEnterpriseMapUpdateEvent(String mapName, Object mergePolicy,
                                       EntryView<Data, Data> entryView, int backupCount) {
        super(mapName, backupCount);
        this.mergePolicy = mergePolicy;

        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<>(entryView);
        }
    }

    public WanEnterpriseMapUpdateEvent() {
    }

    public Object getMergePolicy() {
        return mergePolicy;
    }

    public WanMapEntryView<Data, Data> getEntryView() {
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
    public void incrementEventCount(WanEventCounters counters) {
        counters.incrementUpdate(getMapName());
    }
}
