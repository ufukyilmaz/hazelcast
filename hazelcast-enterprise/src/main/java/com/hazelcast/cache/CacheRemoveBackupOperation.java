package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.BackupOperation;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveBackupOperation extends AbstractOffHeapCacheOperation implements BackupOperation {

    public CacheRemoveBackupOperation() {
    }

    public CacheRemoveBackupOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            response = cache.remove(key, null);
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        dispose();
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REMOVE_BACKUP;
    }

}
