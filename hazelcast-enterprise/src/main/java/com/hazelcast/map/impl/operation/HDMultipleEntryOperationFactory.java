package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HDMultipleEntryOperationFactory implements OperationFactory {

    private String name;
    private Set<Data> keys;
    private EntryProcessor entryProcessor;

    public HDMultipleEntryOperationFactory() {
    }

    public HDMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        this.name = name;
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public Operation createOperation() {
        return new HDMultipleEntryOperation(name, keys, entryProcessor);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeObject(entryProcessor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readUTF();
        int size = in.readInt();
        this.keys = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            keys.add(key);
        }
        this.entryProcessor = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MULTIPLE_ENTRY_FACTORY;
    }
}
