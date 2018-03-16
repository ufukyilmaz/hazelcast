package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class HDPartitionWideEntryWithPredicateOperation extends HDPartitionWideEntryOperation {

    private Predicate predicate;

    public HDPartitionWideEntryWithPredicateOperation() {
    }

    public HDPartitionWideEntryWithPredicateOperation(String name, EntryProcessor entryProcessor, Predicate predicate) {
        super(name, entryProcessor);
        this.predicate = predicate;
    }

    @Override
    protected Predicate getPredicate() {
        return predicate;
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        if (backupProcessor == null) {
            return null;
        }
        if (keysFromIndex != null) {
            // if we used index we leverage it for the backup too
            return new HDMultipleEntryBackupOperation(name, keysFromIndex, backupProcessor);
        } else {
            // if no index used we will do a full partition-scan on backup too
            return new HDPartitionWideEntryBackupOperation(name, backupProcessor);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", entryProcessor='").append(entryProcessor.toString()).append('\'')
                .append(", predicate='").append(predicate.toString()).append('\'');
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY;
    }
}
