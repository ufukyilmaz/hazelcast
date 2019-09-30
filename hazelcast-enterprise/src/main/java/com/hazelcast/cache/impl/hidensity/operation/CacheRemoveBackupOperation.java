package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

/**
 * Removes the backup of a JCache entry.
 */
public class CacheRemoveBackupOperation extends KeyBasedHiDensityCacheOperation
        implements BackupOperation, MutableOperation {

    private boolean wanOriginated;

    public CacheRemoveBackupOperation() {
    }

    public CacheRemoveBackupOperation(String name, Data key) {
        super(name, key, true);
    }

    public CacheRemoveBackupOperation(String name, Data key, boolean wanOriginated) {
        this(name, key);
        this.wanOriginated = wanOriginated;
    }

    @Override
    public void runInternal() {
        if (recordStore != null) {
            response = recordStore.removeRecord(key);
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (!Boolean.FALSE.equals(response) && !wanOriginated) {
            publishWanRemove(key);
        }
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.REMOVE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanOriginated = in.readBoolean();
    }
}
