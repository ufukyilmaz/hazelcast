package com.hazelcast.map.impl.operation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class HDEntryBackupOperation extends HDKeyBasedMapOperation implements BackupOperation {

    private EntryBackupProcessor entryProcessor;

    public HDEntryBackupOperation() {
    }

    public HDEntryBackupOperation(String name, Data dataKey, EntryBackupProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (entryProcessor instanceof HazelcastInstanceAware) {
            HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
            ((HazelcastInstanceAware) entryProcessor).setHazelcastInstance(hazelcastInstance);
        }
    }

    @Override
    protected void runInternal() {
        operator(this, entryProcessor)
                .operateOnKey(dataKey).doPostOperateOps();
    }

    @Override
    public void afterRun() throws Exception {
        disposeDeferredBlocks();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.ENTRY_BACKUP;
    }
}
