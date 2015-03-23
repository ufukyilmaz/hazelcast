package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Base class for {@link com.hazelcast.core.IMap} related WAN
 * replication objects
 */
public abstract class EnterpriseMapReplicationObject implements EnterpriseReplicationEventObject, DataSerializable {

    String groupName;
    String mapName;

    public EnterpriseMapReplicationObject(String mapName, String groupName) {
        this.mapName = mapName;
        this.groupName = groupName;
    }

    public EnterpriseMapReplicationObject() {
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeUTF(groupName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        groupName = in.readUTF();
    }



}
