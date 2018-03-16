package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class HDMultipleEntryWithPredicateOperation extends HDMultipleEntryOperation {

    private Predicate predicate;

    public HDMultipleEntryWithPredicateOperation() {
    }

    public HDMultipleEntryWithPredicateOperation(String name, Set<Data> keys, EntryProcessor entryProcessor,
                                                 Predicate predicate) {
        super(name, keys, entryProcessor);
        this.predicate = checkNotNull(predicate, "predicate cannot be null");
    }

    @Override
    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return new HDMultipleEntryWithPredicateBackupOperation(name, keys, backupProcessor, predicate);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        predicate = in.readObject();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MULTIPLE_ENTRY_PREDICATE;
    }
}
