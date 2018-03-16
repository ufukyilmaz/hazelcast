package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

abstract class AbstractHDMultipleEntryOperation extends HDMapOperation
        implements MutatingOperation, PartitionAwareOperation, Versioned {

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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // RU_COMPAT_3_9
        if (out.getVersion().isLessThan(Versions.V3_10)) {
            out.writeInt(0);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // RU_COMPAT_3_9
        if (in.getVersion().isLessThan(Versions.V3_10)) {
            in.readInt();
        }
    }
}
