package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link IMap} related WAN replication
 * objects.
 */
public abstract class EnterpriseMapReplicationObject
        implements InternalWanReplicationEvent, IdentifiedDataSerializable {

    private Set<String> groupNames = new HashSet<>();
    private String mapName;
    private int backupCount;
    private long creationTime;

    public EnterpriseMapReplicationObject(String mapName, int backupCount) {
        this.mapName = mapName;
        this.backupCount = backupCount;
        this.creationTime = Clock.currentTimeMillis();
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

    @Nonnull
    @Override
    public Set<String> getGroupNames() {
        return groupNames;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(backupCount);
        out.writeInt(groupNames.size());
        for (String groupName : groupNames) {
            out.writeUTF(groupName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        backupCount = in.readInt();
        int groupNameCount = in.readInt();
        for (int i = 0; i < groupNameCount; i++) {
            groupNames.add(in.readUTF());
        }
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public String getObjectName() {
        return mapName;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
