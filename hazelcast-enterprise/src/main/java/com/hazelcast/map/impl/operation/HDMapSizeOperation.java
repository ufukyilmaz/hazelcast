package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

public class HDMapSizeOperation extends HDMapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private int size;

    public HDMapSizeOperation() {
    }

    public HDMapSizeOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordStore.checkIfLoaded();
        size = recordStore.size();
    }

    @Override
    public Object getResponse() {
        return size;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.SIZE;
    }

}
