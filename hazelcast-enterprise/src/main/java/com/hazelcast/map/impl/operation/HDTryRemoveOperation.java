package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDTryRemoveOperation extends HDBaseRemoveOperation implements MutatingOperation {

    private boolean successful;

    public HDTryRemoveOperation() {
    }

    public HDTryRemoveOperation(String name, Data dataKey, long timeout) {
        super(name, dataKey);
        setWaitTimeout(timeout);
    }

    @Override
    protected void runInternal() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        dataOldValue = mapServiceContext.toData(recordStore.remove(dataKey, getCallerProvenance()));
        successful = dataOldValue != null;
    }

    @Override
    public void afterRun() {
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
        return successful;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.TRY_REMOVE;
    }

}
