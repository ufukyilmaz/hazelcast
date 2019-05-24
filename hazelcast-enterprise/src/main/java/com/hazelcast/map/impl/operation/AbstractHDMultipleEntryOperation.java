package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

abstract class AbstractHDMultipleEntryOperation extends HDMapOperation
        implements MutatingOperation, PartitionAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;
    protected EntryBackupProcessor backupProcessor;

    protected AbstractHDMultipleEntryOperation() {
    }

    protected AbstractHDMultipleEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    protected AbstractHDMultipleEntryOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        disposeDeferredBlocks();
    }

    protected Predicate getPredicate() {
        return null;
    }
}
