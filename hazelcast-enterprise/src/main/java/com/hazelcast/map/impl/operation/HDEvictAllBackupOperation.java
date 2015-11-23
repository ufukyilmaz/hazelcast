package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * Operation which evicts all keys except locked ones.
 */
public class HDEvictAllBackupOperation extends HDMapOperation implements BackupOperation, MutatingOperation,
        DataSerializable {

    public HDEvictAllBackupOperation() {
    }

    public HDEvictAllBackupOperation(String name) {
        super(name);
    }

    @Override
    public void runInternal() {
        clearNearCache(false);

        RecordStore recordStore = mapServiceContext.getExistingRecordStore(getPartitionId(), name);
        //if there is no recordStore, then there is nothing to evict.
        if (recordStore == null) {
            return;
        }
        recordStore.evictAll(true);
    }
}
