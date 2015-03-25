package com.hazelcast.cache.operation;

import com.hazelcast.cache.EnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveBackupOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation implementation for cache remove functionality to be used
 * by WAN replication services
 */
public class WanCacheRemoveOperation
        extends AbstractMutatingCacheOperation {

    private String wanGroupName;
    // if same
    private Data oldValue;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, String wanGroupName, Data key,
                                   Data oldValue, int completionId) {
        super(name, key, completionId);
        this.oldValue = oldValue;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void run()
            throws Exception {
        if (oldValue == null) {
            response = ((EnterpriseCacheRecordStore) cache)
                    .remove(key, getCallerUuid(), completionId, wanGroupName);
        } else {
            response = ((EnterpriseCacheRecordStore) cache)
                    .remove(key, oldValue, getCallerUuid(), completionId, wanGroupName);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(oldValue);
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        oldValue = in.readData();
        wanGroupName = in.readUTF();
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.WAN_REMOVE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
