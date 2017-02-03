package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class HDMultipleEntryWithPredicateBackupOperation extends HDMultipleEntryBackupOperation {

    private Predicate predicate;

    public HDMultipleEntryWithPredicateBackupOperation() {
    }

    public HDMultipleEntryWithPredicateBackupOperation(String name, Set<Data> keys,
                                                     EntryBackupProcessor backupProcessor, Predicate predicate) {
        super(name, keys, backupProcessor);
        this.predicate = checkNotNull(predicate, "predicate cannot be null");
    }

    @Override
    protected boolean isEntryProcessable(Map.Entry entry) {
        return super.isEntryProcessable(entry) && predicate.apply(entry);
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
        return EnterpriseMapDataSerializerHook.MULTIPLE_ENTRY_PREDICATE_BACKUP;
    }
}
