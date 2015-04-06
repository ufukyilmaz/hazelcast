package com.hazelcast.map.impl.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * WAN replication object for map remove operations
 */
public class EnterpriseMapReplicationRemove extends EnterpriseMapReplicationObject {

    Data key;
    long removeTime;

    public EnterpriseMapReplicationRemove(String mapName, Data key, long removeTime) {
        super(mapName);
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

}
