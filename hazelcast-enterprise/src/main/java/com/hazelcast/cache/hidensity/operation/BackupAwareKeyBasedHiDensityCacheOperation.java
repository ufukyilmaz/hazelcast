package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;

abstract class BackupAwareKeyBasedHiDensityCacheOperation
        extends KeyBasedHiDensityCacheOperation
        implements BackupAwareOperation {

    protected BackupAwareKeyBasedHiDensityCacheOperation() {
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name) {
        super(name);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key) {
        super(name, key);
    }

    protected BackupAwareKeyBasedHiDensityCacheOperation(String name, Data key, int completionId) {
        super(name, key, completionId);
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

}
