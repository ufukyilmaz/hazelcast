package com.hazelcast.cache.operation;

import com.hazelcast.cache.EnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation implementation for merging entries
 * This operation is used by WAN replication services
 */
public class WanCacheMergeOperation
        extends AbstractMutatingCacheOperation {

    private Data value;
    private long expiryTime;
    private CacheMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName, Data key, Data value, CacheMergePolicy mergePolicy,
                                  long expiryTime, int completionId) {
        super(name, key, completionId);
        this.value = value;
        this.expiryTime = expiryTime;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void run()
            throws Exception {

        response =
                ((EnterpriseCacheRecordStore) cache)
                    .merge(key, value, mergePolicy, expiryTime, getCallerUuid(), completionId, wanGroupName);

        if (Boolean.TRUE.equals(response)) {
            backupRecord = cache.getRecord(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeLong(expiryTime);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readData();
        expiryTime = in.readLong();
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.WAN_MERGE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
