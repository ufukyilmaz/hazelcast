package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDReplaceIfSameOperation extends HDBasePutOperation implements MutatingOperation {

    private Data expect;
    private boolean successful;

    public HDReplaceIfSameOperation() {
    }

    public HDReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        super(name, dataKey, update);
        this.expect = expect;
    }

    @Override
    protected void runInternal() {
        successful = recordStore.replace(dataKey, expect, dataValue);
        if (successful) {
            oldValue = expect;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (successful) {
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
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(expect);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readData();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.REPLACE_IF_SAME;
    }

}
