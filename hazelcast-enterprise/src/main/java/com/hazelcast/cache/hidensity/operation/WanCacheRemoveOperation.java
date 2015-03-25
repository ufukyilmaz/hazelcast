package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation implementation for cache remove functionality to be used
 * by WAN replication services
 */
public class WanCacheRemoveOperation extends BackupAwareHiDensityCacheOperation {

    private Data currentValue;
    private String wanGroupName;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, String wanGroupName, Data key,
                                   Data currentValue, int completionId) {
        super(name, key, completionId);
        this.currentValue = currentValue;
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.remove(key, getCallerUuid(), completionId, wanGroupName);
            } else {
                response = cache.remove(key, currentValue, getCallerUuid(), completionId, wanGroupName);
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        dispose();
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        if (currentValue != null) {
            binaryService.disposeData(currentValue);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(currentValue);
        out.writeUTF(wanGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = readOperationData(in);
        wanGroupName = in.readUTF();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.WAN_REMOVE;
    }
}
