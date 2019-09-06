package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.wan.impl.CallerProvenance.WAN;

/**
 * Operation implementation for cache remove functionality to be used
 * by WAN replication services.
 */
public class WanCacheRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation {

    private String wanGroupName;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, String wanGroupName, Data key, int completionId) {
        super(name, key, completionId);
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() {
        response = recordStore.remove(key, getCallerUuid(), wanGroupName, completionId, WAN);
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
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanGroupName = in.readUTF();
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.WAN_REMOVE;
    }

}