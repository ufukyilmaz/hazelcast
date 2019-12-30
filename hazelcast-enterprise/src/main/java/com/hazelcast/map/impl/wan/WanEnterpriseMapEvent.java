package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.impl.InternalWanEvent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link IMap} related WAN replication
 * objects.
 *
 * @param <T> type of event data
 */
public abstract class WanEnterpriseMapEvent<T>
        implements InternalWanEvent<T>, IdentifiedDataSerializable {

    private Set<String> clusterNames = new HashSet<>();
    private String mapName;
    private int backupCount;
    private long creationTime;


    public WanEnterpriseMapEvent(String mapName, int backupCount) {
        this.mapName = mapName;
        this.backupCount = backupCount;
        this.creationTime = Clock.currentTimeMillis();
    }

    public WanEnterpriseMapEvent() {
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
    public Set<String> getClusterNames() {
        return clusterNames;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(backupCount);
        out.writeInt(clusterNames.size());
        for (String clusterName : clusterNames) {
            out.writeUTF(clusterName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        backupCount = in.readInt();
        int clusterNameCount = in.readInt();
        for (int i = 0; i < clusterNameCount; i++) {
            clusterNames.add(in.readUTF());
        }
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Nonnull
    @Override
    public String getObjectName() {
        return mapName;
    }

    @Nonnull
    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
