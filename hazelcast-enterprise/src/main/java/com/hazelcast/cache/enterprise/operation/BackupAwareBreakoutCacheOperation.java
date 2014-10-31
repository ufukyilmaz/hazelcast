package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;

/**
 * @author mdogan 05/02/14
 */
abstract class BackupAwareBreakoutCacheOperation
        extends AbstractBreakoutCacheOperation
        implements BackupAwareOperation {

    protected BackupAwareBreakoutCacheOperation() {
    }

    protected BackupAwareBreakoutCacheOperation(String name) {
        super(name);
    }

    protected BackupAwareBreakoutCacheOperation(String name, Data key) {
        super(name, key);
    }

    protected BackupAwareBreakoutCacheOperation(String name, Data key, int completionId) {
        super(name, key, completionId);
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
