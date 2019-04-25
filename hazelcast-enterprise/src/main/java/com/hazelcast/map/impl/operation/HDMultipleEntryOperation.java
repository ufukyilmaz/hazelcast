package com.hazelcast.map.impl.operation;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class HDMultipleEntryOperation extends AbstractHDMultipleEntryOperation implements BackupAwareOperation {

    protected Set<Data> keys;
    protected transient EntryOperator operator;

    public HDMultipleEntryOperation() {
    }

    public HDMultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name, entryProcessor);
        this.keys = keys;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        responses = new MapEntries(keys.size());

        operator = operator(this, entryProcessor, getPredicate());
        for (Data key : keys) {
            Data response = operator.operateOnKey(key).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(key, response);
            }
        }
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        if (backupProcessor == null) {
            return null;
        }

        return new HDMultipleEntryBackupOperation(name, keys, backupProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
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
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MULTIPLE_ENTRY;
    }

}
