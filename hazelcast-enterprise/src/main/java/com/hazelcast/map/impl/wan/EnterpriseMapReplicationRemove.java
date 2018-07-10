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
    private long removeTime;

    public EnterpriseMapReplicationRemove(String mapName, Data key, long removeTime, int backupCount) {
        super(mapName, backupCount);
        this.key = key;
        this.removeTime = removeTime;
    }

    public EnterpriseMapReplicationRemove() {
    }

    public long getRemoveTime() {
        return removeTime;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(removeTime);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        removeTime = in.readLong();
        key = in.readData();
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementRemove(getMapName());
    }
}
