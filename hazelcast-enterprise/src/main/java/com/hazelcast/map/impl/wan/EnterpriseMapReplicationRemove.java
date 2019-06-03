package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.io.IOException;

/**
 * WAN replication object for map remove operations.
 */
public class EnterpriseMapReplicationRemove extends EnterpriseMapReplicationObject {
    private Data key;

    public EnterpriseMapReplicationRemove(String mapName, Data key, int backupCount) {
        super(mapName, backupCount);
        this.key = key;
    }

    public EnterpriseMapReplicationRemove() {
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        key = in.readData();
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementRemove(getMapName());
    }
}
