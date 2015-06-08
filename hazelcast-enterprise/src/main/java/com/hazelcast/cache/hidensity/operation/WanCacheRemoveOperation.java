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

    private String wanGroupName;

    public WanCacheRemoveOperation() {
    }

    public WanCacheRemoveOperation(String name, String wanGroupName, Data key, int completionId) {
        super(name, key, completionId);
        this.wanGroupName = wanGroupName;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
                response = cache.remove(key, getCallerUuid(), completionId, wanGroupName);
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.WAN_REMOVE;
    }
}
