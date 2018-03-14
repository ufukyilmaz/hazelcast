package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

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
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName, SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy,
                                  CacheMergeTypes mergingEntry, int completionId) {
        super(name, completionId);
        this.mergingEntry = mergingEntry;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() {
        if (cache.merge(mergingEntry, mergePolicy) != null) {
            response = true;
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        ExpiryPolicy expiryPolicy = null;
        long expiryTime = mergingEntry.getExpirationTime();
        if (expiryTime > 0) {
            long ttl = expiryTime - System.currentTimeMillis();
            expiryPolicy = new HazelcastExpiryPolicy(ttl, 0L, 0L);
        }
        return new CachePutBackupOperation(name, mergingEntry.getKey(), mergingEntry.getValue(), expiryPolicy, true);
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
        return HiDensityCacheDataSerializerHook.WAN_MERGE;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
