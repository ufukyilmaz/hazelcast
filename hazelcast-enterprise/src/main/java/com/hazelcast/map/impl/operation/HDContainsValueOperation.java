package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class HDContainsValueOperation extends HDMapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private boolean contains;
    private Data testValue;

    public HDContainsValueOperation(String name, Data testValue) {
        super(name);
        this.testValue = testValue;
    }

    public HDContainsValueOperation() {
    }

    @Override
    protected void runInternal() {
        contains = recordStore.containsValue(testValue);
    }

    @Override
    public Object getResponse() {
        return contains;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(testValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = in.readData();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.CONTAINS_VALUE;
    }
}
