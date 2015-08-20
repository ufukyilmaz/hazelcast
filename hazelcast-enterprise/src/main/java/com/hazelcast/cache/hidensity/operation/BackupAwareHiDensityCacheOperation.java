package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.spi.BackupAwareOperation;

/**
 * @author sozal 07/08/15
 */
abstract class BackupAwareHiDensityCacheOperation
        extends AbstractHiDensityCacheOperation
        implements BackupAwareOperation {

    protected BackupAwareHiDensityCacheOperation() {
    }

    protected BackupAwareHiDensityCacheOperation(String name) {
        super(name);
    }

    protected BackupAwareHiDensityCacheOperation(String name,
                                                 boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, dontCreateCacheRecordStoreIfNotExist);
    }

    protected BackupAwareHiDensityCacheOperation(String name, int completionId) {
        super(name, completionId);
    }

    protected BackupAwareHiDensityCacheOperation(String name, int completionId,
                                              boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, completionId, dontCreateCacheRecordStoreIfNotExist);
    }

    @Override
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

}
