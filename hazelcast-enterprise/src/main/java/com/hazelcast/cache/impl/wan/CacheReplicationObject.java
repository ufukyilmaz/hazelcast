package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
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
 * Base class for {@link com.hazelcast.cache.ICache} related WAN replication objects.
 */
public abstract class CacheReplicationObject implements InternalWanReplicationEvent, IdentifiedDataSerializable {

    private String cacheName;
    private String managerPrefix;
    private Set<String> groupNames = new HashSet<>();
    private int backupCount;
    private long creationTime;

    public CacheReplicationObject(String cacheName, String managerPrefix, int backupCount) {
        this.cacheName = cacheName;
        this.managerPrefix = managerPrefix;
        this.backupCount = backupCount;
        this.creationTime = Clock.currentTimeMillis();
    }

    public CacheReplicationObject() {
    }

    public String getManagerPrefix() {
        return managerPrefix;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getNameWithPrefix() {
        return managerPrefix + cacheName;
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
        out.writeUTF(cacheName);
        out.writeUTF(managerPrefix);
        out.writeInt(backupCount);
        out.writeInt(groupNames.size());
        for (String groupName : groupNames) {
            out.writeUTF(groupName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readUTF();
        managerPrefix = in.readUTF();
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
        return cacheName;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }
}
