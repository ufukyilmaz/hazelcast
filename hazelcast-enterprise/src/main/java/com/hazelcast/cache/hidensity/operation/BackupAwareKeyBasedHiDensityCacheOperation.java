package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;

/**
 * @author mdogan 05/02/14
 */
abstract class BackupAwareKeyBasedHiDensityCacheOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements BackupAwareOperation {

    protected BackupAwareKeyBasedHiDensityCacheOperation() {
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name) {
        super(name);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, boolean dontCreateCacheRecordStore) {
        super(name, dontCreateCacheRecordStore);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key) {
        super(name, key);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key,
                                                         boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, key, dontCreateCacheRecordStoreIfNotExist);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, int completionId) {
        super(name, completionId);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, int completionId,
                                                         boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, completionId, dontCreateCacheRecordStoreIfNotExist);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key, int completionId) {
        super(name, key, completionId);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key, int completionId,
                                                         boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, key, completionId, dontCreateCacheRecordStoreIfNotExist);
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
