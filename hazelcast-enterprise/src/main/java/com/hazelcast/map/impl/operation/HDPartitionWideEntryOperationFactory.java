package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class HDPartitionWideEntryOperationFactory implements OperationFactory {

    private String name;
    private EntryProcessor entryProcessor;

    public HDPartitionWideEntryOperationFactory() {
    }

    public HDPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        this.name = name;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public Operation createOperation() {
        return new HDPartitionWideEntryOperation(name, entryProcessor);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entryProcessor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entryProcessor = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_ENTRY_FACTORY;
    }
}
