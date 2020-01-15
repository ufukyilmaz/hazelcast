package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.merge.CacheMergingEntryImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import java.io.IOException;

/**
 * Operation implementation for merging entries.
 * This operation is used by WAN replication services.
 *
 * @since 3.10
 */
public class WanCacheMergeOperation extends MutatingCacheOperation {

    private CacheMergeTypes<Object, Object> mergingEntry;
    private SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, CacheMergeTypes<Object, Object> mergingEntry,
                                  SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy,
                                  int completionId) {
        super(name, ((CacheMergingEntryImpl) mergingEntry).getRawKey(), completionId);
        this.mergingEntry = mergingEntry;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void run() throws Exception {
        CacheRecord record = recordStore.merge(mergingEntry, mergePolicy, CallerProvenance.WAN);
        if (record != null) {
            response = true;
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
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return EnterpriseCacheDataSerializerHook.WAN_MERGE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
