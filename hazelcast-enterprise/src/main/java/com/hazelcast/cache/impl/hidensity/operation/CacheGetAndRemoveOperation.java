package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Atomically removes the entry for a key only if it is currently mapped to some value.
 */
public class CacheGetAndRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    public CacheGetAndRemoveOperation() {
    }

    public CacheGetAndRemoveOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    protected void runInternal() {
        response = recordStore.getAndRemove(key, getCallerUuid(), completionId);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
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
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.GET_AND_REMOVE;
    }
}
