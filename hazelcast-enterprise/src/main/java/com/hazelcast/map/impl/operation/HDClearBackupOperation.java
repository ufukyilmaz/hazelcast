package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDClearBackupOperation extends HDMapOperation implements BackupOperation, MutatingOperation {

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
