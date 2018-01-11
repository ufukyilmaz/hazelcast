package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.BackupOperation;

/**
 * Operation which evicts all keys except locked ones.
 */
public class HDEvictAllBackupOperation extends HDMapOperation implements BackupOperation {

    public HDEvictAllBackupOperation() {
        this(null);
    }

    public HDEvictAllBackupOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    public void runInternal() {
        if (recordStore != null) {
            recordStore.evictAll(true);
        }
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.EVICT_ALL_BACKUP;
    }
}
