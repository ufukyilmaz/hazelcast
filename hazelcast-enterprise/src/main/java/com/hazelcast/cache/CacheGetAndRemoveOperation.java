package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

/**
 * @author mdogan 05/02/14
 */
public class CacheGetAndRemoveOperation extends BackupAwareOffHeapCacheOperation {

    public CacheGetAndRemoveOperation() {
    }

    public CacheGetAndRemoveOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public void runInternal() throws Exception {
        response = cache != null ? cache.getAndRemove(key, getCallerUuid()) : null;
    }

    @Override
    public void afterRun() throws Exception {
        dispose();
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
    protected void disposeInternal(SerializationService binaryService) {
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_AND_REMOVE;
    }
}
