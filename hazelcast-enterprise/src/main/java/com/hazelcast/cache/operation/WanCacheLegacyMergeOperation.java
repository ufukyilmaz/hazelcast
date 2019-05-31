package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
import com.hazelcast.cache.impl.operation.MutatingCacheOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.impl.CallerProvenance;

import java.io.IOException;

import static java.lang.Boolean.TRUE;

/**
 * Operation implementation for merging entries.
 * This operation is used by WAN replication services.
 */
public class WanCacheLegacyMergeOperation extends MutatingCacheOperation {

    private CacheEntryView<Data, Data> cacheEntryView;
    private CacheMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheLegacyMergeOperation() {
    }

    public WanCacheLegacyMergeOperation(String name, String wanGroupName,
                                        CacheEntryView<Data, Data> cacheEntryView,
                                        CacheMergePolicy mergePolicy, int completionId) {
        super(name, cacheEntryView.getKey(), completionId);
        this.cacheEntryView = cacheEntryView;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void run() throws Exception {
        CacheRecord record = recordStore.merge(cacheEntryView, mergePolicy,
                getCallerUuid(), wanGroupName, completionId, CallerProvenance.WAN);
        response = record != null;

        if (TRUE.equals(response)) {
            backupRecord = recordStore.getRecord(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        return backupRecord != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
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
        return EnterpriseCacheDataSerializerHook.WAN_LEGACY_MERGE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
