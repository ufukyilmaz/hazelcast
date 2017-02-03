package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;

public class HDReplaceOperation extends HDBasePutOperation {

    private boolean successful;

    public HDReplaceOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public HDReplaceOperation() {
    }

    @Override
    protected void runInternal() {
        Object oldValue = recordStore.replace(dataKey, dataValue);
        dataOldValue = mapService.getMapServiceContext().toData(oldValue);
        successful = oldValue != null;
    }

    @Override
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
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
        return dataOldValue;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.REPLACE;
    }
}
