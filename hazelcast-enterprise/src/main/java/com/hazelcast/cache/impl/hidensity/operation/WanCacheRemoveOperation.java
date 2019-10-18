package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.wan.impl.CallerProvenance.WAN;

/**
 * Operation implementation for cache remove functionality to be used
 * by WAN replication services.
 */
public class WanCacheRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation {

    private UUID origin;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, UUID origin, Data key, int completionId) {
        super(name, key, completionId);
        this.origin = origin;
    }

    @Override
    public void runInternal() {
        response = recordStore.remove(key, getCallerUuid(), origin, completionId, WAN);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        dispose();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, origin);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        origin = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.WAN_REMOVE;
    }

}