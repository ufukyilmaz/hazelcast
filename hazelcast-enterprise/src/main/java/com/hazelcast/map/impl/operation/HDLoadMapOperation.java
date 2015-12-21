package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Triggers map loading from a map store
 */
public class HDLoadMapOperation extends HDMapOperation {

    private boolean replaceExistingValues;

    public HDLoadMapOperation() {
    }

    public HDLoadMapOperation(String name, boolean replaceExistingValues) {
        super(name);
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    protected void runInternal() {
        recordStore.loadAll(replaceExistingValues);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replaceExistingValues = in.readBoolean();
    }
}
