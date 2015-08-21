package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveBackupOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    public CacheRemoveBackupOperation() {
    }

    public CacheRemoveBackupOperation(String name, Data key) {
        super(name, key, true);
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            response = cache.removeRecord(key);
        } else {
            response = Boolean.FALSE;
        }
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.REMOVE_BACKUP;
    }

}
