package com.hazelcast.cache.operation;

import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergingEntry;

import java.io.IOException;

import static java.lang.Boolean.TRUE;

/**
 * Operation implementation for merging entries.
 * This operation is used by WAN replication services.
 *
 * @since 3.10
 */
public class WanCacheMergeOperation
        extends AbstractMutatingCacheOperation {

    private MergingEntry<Data, Data> mergingEntry;
    private SplitBrainMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName, MergingEntry<Data, Data> mergingEntry,
                                  SplitBrainMergePolicy mergePolicy, int completionId) {
        super(name, mergingEntry.getKey(), completionId);
        this.mergingEntry = mergingEntry;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void run() throws Exception {
        CacheRecord record = cache.merge(mergingEntry, mergePolicy);
        if (record != null) {
            response = true;
            backupRecord = cache.getRecord(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        return TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
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
