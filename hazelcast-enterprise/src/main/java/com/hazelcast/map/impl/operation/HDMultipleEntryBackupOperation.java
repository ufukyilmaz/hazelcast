package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class HDMultipleEntryBackupOperation extends AbstractHDMultipleEntryOperation implements BackupOperation {

    private Set<Data> keys;

    public HDMultipleEntryBackupOperation() {
    }

    public HDMultipleEntryBackupOperation(String name, Set<Data> keys, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
        this.keys = keys;
    }

    @Override
    protected void runInternal() {
        EntryOperator operator = operator(this, backupProcessor, getPredicate());
        for (Data key : keys) {
            operator.operateOnKey(key).doPostOperateOps();
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
        int size = in.readInt();
        keys = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            keys.add(key);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MULTIPLE_ENTRY_BACKUP;
    }

}
