package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDDeleteOperation extends HDBaseRemoveOperation
        implements MutatingOperation, Versioned {

    private boolean success;

    public HDDeleteOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey, disableWanReplicationEvent);
    }

    public HDDeleteOperation() {
    }

    @Override
    protected void runInternal() {
        success = recordStore.delete(dataKey, getCallerProvenance());
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public void afterRun() {
        if (success) {
            super.afterRun();
        }

        disposeDeferredBlocks();
    }

    @Override
    public boolean shouldBackup() {
        return success;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.DELETE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        disableWanReplicationEvent = in.readBoolean();
    }
}
