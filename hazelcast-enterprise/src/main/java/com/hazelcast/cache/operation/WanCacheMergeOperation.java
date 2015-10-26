package com.hazelcast.cache.operation;

import com.hazelcast.cache.EnterpriseCacheRecordStore;
import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
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

    private CacheEntryView<Data, Data> cacheEntryView;
    private CacheMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName, CacheEntryView<Data, Data> cacheEntryView,
                                  CacheMergePolicy mergePolicy, int completionId) {
        super(name, cacheEntryView.getKey(), completionId);
        this.cacheEntryView = cacheEntryView;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void run()
            throws Exception {
        response = ((EnterpriseCacheRecordStore) cache)
                        .merge(cacheEntryView, mergePolicy, getCallerUuid(),
                               completionId, wanGroupName);

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
        return new CachePutBackupOperation(name, key, backupRecord, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(cacheEntryView);
        out.writeObject(mergePolicy);
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cacheEntryView = in.readObject();
        mergePolicy = in.readObject();
        wanGroupName = in.readUTF();
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
