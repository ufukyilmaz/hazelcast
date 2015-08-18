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
    int backupCount;

    public EnterpriseMapReplicationObject(String mapName, int backupCount) {
        this.mapName = mapName;
        this.backupCount = backupCount;
    }

    public EnterpriseMapReplicationObject() {
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public Set<String> getGroupNames() {
        return groupNames;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(backupCount);
        out.writeObject(groupNames);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        backupCount = in.readInt();
        groupNames = in.readObject();
    }
}
