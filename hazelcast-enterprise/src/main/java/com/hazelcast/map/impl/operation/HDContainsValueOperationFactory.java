package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public final class HDContainsValueOperationFactory implements OperationFactory {

    private String name;
    private Data value;

    public HDContainsValueOperationFactory() {
    }

    public HDContainsValueOperationFactory(String name, Data value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public Operation createOperation() {
        return new HDContainsValueOperation(name, value);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeData(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        value = in.readData();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.CONTAINS_VALUE_FACTORY;
    }
}
