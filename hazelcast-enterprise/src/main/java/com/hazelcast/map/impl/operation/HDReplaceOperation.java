package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDReplaceOperation extends HDBasePutOperation implements MutatingOperation {

    private boolean successful;

    public HDReplaceOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public HDReplaceOperation() {
    }

    @Override
    protected void runInternal() {
        Object oldValue = recordStore.replace(dataKey, dataValue);
        this.oldValue = mapService.getMapServiceContext().toData(oldValue);
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
        return oldValue;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.REPLACE;
    }
}
