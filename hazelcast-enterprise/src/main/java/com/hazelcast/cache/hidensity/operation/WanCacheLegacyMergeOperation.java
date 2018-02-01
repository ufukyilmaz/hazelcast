package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Operation implementation for merging entries.
 * This operation is used by WAN replication services.
 */
public class WanCacheLegacyMergeOperation
        extends BackupAwareHiDensityCacheOperation
        implements MutableOperation {

    private CacheEntryView<Data, Data> cacheEntryView;
    private CacheMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheLegacyMergeOperation() {
    }

    public WanCacheLegacyMergeOperation(String name, String wanGroupName,
                                        CacheMergePolicy mergePolicy,
                                        CacheEntryView<Data, Data> cacheEntryView, int completionId) {
        super(name, completionId);
        this.cacheEntryView = cacheEntryView;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() {
        CacheRecord record = cache.merge(cacheEntryView, mergePolicy, getCallerUuid(), wanGroupName, completionId);
        response = record != null;
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        ExpiryPolicy expiryPolicy = null;
        long expiryTime = cacheEntryView.getExpirationTime();
        if (expiryTime > 0) {
            long ttl = expiryTime - System.currentTimeMillis();
            expiryPolicy = new HazelcastExpiryPolicy(ttl, 0L, 0L);
        }
        return new CachePutBackupOperation(name, cacheEntryView.getKey(), cacheEntryView.getValue(), expiryPolicy, true);
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
        return HiDensityCacheDataSerializerHook.WAN_LEGACY_MERGE;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
