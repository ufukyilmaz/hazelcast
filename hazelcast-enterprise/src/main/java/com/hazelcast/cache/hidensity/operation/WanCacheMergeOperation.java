package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Operation implementation for merging entries.
 * This operation is used by WAN replication services.
 *
 * @since 3.10
 */
public class WanCacheMergeOperation
        extends BackupAwareHiDensityCacheOperation
        implements MutableOperation {

    private CacheMergeTypes mergingEntry;
    private SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name,
                                  SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy,
                                  CacheMergeTypes mergingEntry, int completionId) {
        super(name, completionId);
        this.mergingEntry = mergingEntry;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void runInternal() {
        if (recordStore.merge(mergingEntry, mergePolicy, CallerProvenance.WAN) != null) {
            response = true;
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response) && recordStore.getRecord(mergingEntry.getKey()) != null;
    }

    @Override
    public Operation getBackupOperation() {
        ExpiryPolicy expiryPolicy = createOrNullBackupExpiryPolicy(mergingEntry.getExpirationTime());
        CacheRecord record = recordStore.getRecord(mergingEntry.getKey());
        return new CachePutBackupOperation(name, mergingEntry.getKey(),
                mergingEntry.getValue(), expiryPolicy, record.getCreationTime(), true);
    }

    static ExpiryPolicy createOrNullBackupExpiryPolicy(long expiryTime) {
        if (expiryTime <= 0) {
            return null;
        }

        long ttl = expiryTime - Clock.currentTimeMillis();
        if (ttl <= 0) {
            return null;
        }

        return new HazelcastExpiryPolicy(ttl, 0L, 0L);
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
        return HiDensityCacheDataSerializerHook.WAN_MERGE;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
