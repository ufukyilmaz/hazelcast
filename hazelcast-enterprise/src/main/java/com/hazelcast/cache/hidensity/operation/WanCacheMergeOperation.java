package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

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

    private SplitBrainMergeEntryView<Data, Data> mergeEntryView;
    private SplitBrainMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName,
                                  SplitBrainMergePolicy mergePolicy,
                                  SplitBrainMergeEntryView<Data, Data> mergeEntryView, int completionId) {
        super(name, completionId);
        this.mergeEntryView = mergeEntryView;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() {
        if (cache.merge(mergeEntryView, mergePolicy) != null) {
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
        long expiryTime = mergeEntryView.getExpirationTime();
        if (expiryTime > 0) {
            long ttl = expiryTime - System.currentTimeMillis();
            expiryPolicy = new HazelcastExpiryPolicy(ttl, 0L, 0L);
        }
        return new CachePutBackupOperation(name, mergeEntryView.getKey(), mergeEntryView.getValue(), expiryPolicy, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergeEntryView);
        out.writeObject(mergePolicy);
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergeEntryView = in.readObject();
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
