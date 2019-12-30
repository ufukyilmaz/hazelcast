package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.impl.InternalWanEvent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link com.hazelcast.cache.ICache} related WAN replication objects.
 *
 * @param <T> type of event data
 */
public abstract class WanEnterpriseCacheEvent<T>
        implements InternalWanEvent<T>, IdentifiedDataSerializable {
    private String cacheName;
    private String managerPrefix;
    private Set<String> clusterNames = new HashSet<>();
    private int backupCount;
    private long creationTime;

    public WanEnterpriseCacheEvent(@Nonnull String cacheName,
                                   @Nonnull String managerPrefix,
                                   int backupCount) {
        this.cacheName = cacheName;
        this.managerPrefix = managerPrefix;
        this.backupCount = backupCount;
        this.creationTime = Clock.currentTimeMillis();
    }

    public WanEnterpriseCacheEvent() {
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
    public Set<String> getClusterNames() {
        return clusterNames;
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
        out.writeInt(clusterNames.size());
        for (String clusterName : clusterNames) {
            out.writeUTF(clusterName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readUTF();
        managerPrefix = in.readUTF();
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
        return cacheName;
    }

    @Nonnull
    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }
}
