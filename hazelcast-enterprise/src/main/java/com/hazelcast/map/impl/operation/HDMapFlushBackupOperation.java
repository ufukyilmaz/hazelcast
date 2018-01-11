package com.hazelcast.map.impl.operation;

import com.hazelcast.core.IMap;
import com.hazelcast.spi.BackupOperation;

/**
 * Empties backup write-behind-queues upon {@link IMap#flush()}
 */
public class HDMapFlushBackupOperation extends HDMapOperation implements BackupOperation {

    public HDMapFlushBackupOperation() {
    }

    public HDMapFlushBackupOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordStore.softFlush();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.FLUSH_BACKUP;
    }
}
