package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.WanEventCounters;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * WAN replication object for map remove operations.
 */
public class WanEnterpriseMapRemoveEvent extends WanEnterpriseMapEvent {
    private Data key;

    public WanEnterpriseMapRemoveEvent(String mapName, Data key, int backupCount) {
        super(mapName, backupCount);
        this.key = key;
    }

    public WanEnterpriseMapRemoveEvent() {
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
        return WanDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(WanEventCounters counters) {
        counters.incrementRemove(getMapName());
    }
}
