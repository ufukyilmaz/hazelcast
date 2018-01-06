package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDDeleteOperation extends HDBaseRemoveOperation implements MutatingOperation {
    private boolean success;

    public HDDeleteOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey, disableWanReplicationEvent);
    }

    public HDDeleteOperation() {
    }

    @Override
    protected void runInternal() {
        success = recordStore.delete(dataKey);
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
}
