package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDMapFlushOperation extends HDMapOperation implements PartitionAwareOperation, MutatingOperation {

    public HDMapFlushOperation() {
    }

    public HDMapFlushOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordStore.flush();
    }

    @Override
    public Object getResponse() {
        return true;
    }

}
