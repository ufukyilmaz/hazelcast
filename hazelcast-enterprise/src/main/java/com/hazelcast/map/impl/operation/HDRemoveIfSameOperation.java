package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDRemoveIfSameOperation extends HDBaseRemoveOperation implements MutatingOperation {

    private Data testValue;
    private boolean successful;

    public HDRemoveIfSameOperation() {
    }

    public HDRemoveIfSameOperation(String name, Data dataKey, Data value) {
        super(name, dataKey);
        testValue = value;
    }

    @Override
    protected void runInternal() {
        successful = recordStore.remove(dataKey, testValue);
    }

    @Override
    public void afterRun() {
        if (successful) {
            dataOldValue = testValue;
            super.afterRun();
        }

        disposeDeferredBlocks();
    }

    @Override
    public Object getResponse() {
        return successful;
    }

    @Override
    public boolean shouldBackup() {
        return successful;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
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
        return EnterpriseMapDataSerializerHook.REMOVE_IF_SAME;
    }
}
