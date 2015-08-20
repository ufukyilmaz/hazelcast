package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.Operation;

/**
 * @author mdogan 05/02/14
 */
public class CacheGetAndRemoveOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation {

    public CacheGetAndRemoveOperation() {
    }

    public CacheGetAndRemoveOperation(String name, Data key) {
        super(name, key, true);
    }

    @Override
    protected void runInternal() throws Exception {
        response = cache != null ? cache.getAndRemove(key, getCallerUuid(), completionId) : null;
    }

    @Override
    public void afterRun() throws Exception {
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.GET_AND_REMOVE;
    }

}
