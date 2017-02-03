package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;

public class HDTryPutOperation extends HDBasePutOperation {

    public HDTryPutOperation() {
    }

    public HDTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        super(name, dataKey, value);
        setWaitTimeout(timeout);
    }

    @Override
    protected void runInternal() {
        recordStore.put(dataKey, dataValue, ttl);
    }

    @Override
    public boolean shouldBackup() {
        return recordStore.getRecord(dataKey) != null;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.TRY_PUT;
    }

}
