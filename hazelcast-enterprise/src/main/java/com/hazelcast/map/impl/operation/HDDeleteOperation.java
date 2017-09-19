package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;

public class HDDeleteOperation extends HDBaseRemoveOperation {
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
