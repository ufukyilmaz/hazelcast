package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;

public class HDPartitionWideEntryWithPredicateBackupOperation extends HDPartitionWideEntryBackupOperation {

    private Predicate predicate;

    public HDPartitionWideEntryWithPredicateBackupOperation() {
    }

    public HDPartitionWideEntryWithPredicateBackupOperation(String name, EntryBackupProcessor entryProcessor,
                                                            Predicate predicate) {
        super(name, entryProcessor);
        this.predicate = predicate;
    }

    protected Predicate getPredicate() {
        return predicate;
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
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY_BACKUP;
    }
}
