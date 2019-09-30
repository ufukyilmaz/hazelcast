package com.hazelcast.cache.impl.hidensity.operation;


import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;

abstract class BackupAwareHiDensityCacheOperation extends HiDensityCacheOperation
        implements BackupAwareOperation {

    protected BackupAwareHiDensityCacheOperation() {
    }

    protected BackupAwareHiDensityCacheOperation(String name) {
        super(name);
    }

    protected BackupAwareHiDensityCacheOperation(String name, int completionId) {
        super(name, completionId);
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }
}
