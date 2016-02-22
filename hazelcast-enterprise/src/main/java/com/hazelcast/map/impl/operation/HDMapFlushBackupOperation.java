package com.hazelcast.map.impl.operation;

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * Empties backup write-behind-queues upon {@link IMap#flush()}
 */
public class HDMapFlushBackupOperation extends HDMapOperation implements BackupOperation, MutatingOperation {

    public HDMapFlushBackupOperation() {
    }

    public HDMapFlushBackupOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        recordStore.softFlush();
    }
}
