package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.BackupOperation;

public class HDClearBackupOperation extends HDMapOperation implements BackupOperation {

    public HDClearBackupOperation() {
        this(null);
    }

    public HDClearBackupOperation(String name) {
        super(name);
        this.createRecordStoreOnDemand = false;
    }


    @Override
    protected void runInternal() {
        if (recordStore != null) {
            recordStore.clear();
        }
    }

    @Override
    public void afterRun() throws Exception {
        disposeDeferredBlocks();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.CLEAR_BACKUP;
    }
}
