package com.hazelcast.cache.impl.operation;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.impl.CallerProvenance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * Operation implementation for cache remove functionality to be used
 * by WAN replication services.
 */
public class WanCacheRemoveOperation extends MutatingCacheOperation {

    private String origin;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, String origin,
                                   Data key, int completionId) {
        super(name, key, completionId);
        this.origin = origin;
    }

    @Override
    public void run() throws Exception {
        response = recordStore.remove(key, getCallerUuid(),
                origin, completionId, CallerProvenance.WAN);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(origin);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        origin = in.readUTF();
    }

    @Override
    public int getClassId() {
        return EnterpriseCacheDataSerializerHook.WAN_REMOVE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
