package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;

/**
 * @author mdogan 05/02/14
 */
abstract class BackupAwareOffHeapCacheOperation extends AbstractOffHeapCacheOperation implements BackupAwareOperation {

    protected BackupAwareOffHeapCacheOperation() {
    }

    protected BackupAwareOffHeapCacheOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }
}
