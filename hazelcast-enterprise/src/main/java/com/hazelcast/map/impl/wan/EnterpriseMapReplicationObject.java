package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link com.hazelcast.core.IMap} related WAN
 * replication objects
 */
public abstract class EnterpriseMapReplicationObject implements EnterpriseReplicationEventObject, DataSerializable {

    Set<String> groupNames = new HashSet<String>();
    String mapName;

    public EnterpriseMapReplicationObject(String mapName) {
        this.mapName = mapName;
    }

    public EnterpriseMapReplicationObject() {
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public Set<String> getGroupNames() {
        return groupNames;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(groupNames);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        groupNames = in.readObject();
    }
}
