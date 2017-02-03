package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * Operation factory for evict all operations.
 */
public class HDEvictAllOperationFactory implements OperationFactory {

    private String name;

    public HDEvictAllOperationFactory() {
    }

    public HDEvictAllOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new HDEvictAllOperation(name);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.EVICT_ALL_FACTORY;
    }
}
