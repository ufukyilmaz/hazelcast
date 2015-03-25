package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Operation implementation for merging entries
 * This operation is used by WAN replication services
 */
public class WanCacheMergeOperation extends BackupAwareHiDensityCacheOperation {

    private Data value;
    private long expiryTime;
    private CacheMergePolicy mergePolicy;
    private String wanGroupName;

    public WanCacheMergeOperation() {
    }

    public WanCacheMergeOperation(String name, String wanGroupName, Data key, Data value,
                                  CacheMergePolicy mergePolicy, long expiryTime, int completionId) {
        super(name, key, completionId);
        this.value = value;
        this.expiryTime = expiryTime;
        this.mergePolicy = mergePolicy;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal()
            throws Exception {
        response = cache.merge(key, value, mergePolicy, expiryTime,
                getCallerUuid(), completionId, wanGroupName);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        ExpiryPolicy expiryPolicy = null;
        if (expiryTime > 0) {
            long ttl = expiryTime - System.currentTimeMillis();
            expiryPolicy = new HazelcastExpiryPolicy(ttl, 0L, 0L);
        }
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
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
        return HiDensityCacheDataSerializerHook.WAN_MERGE;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }
}
