package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;

public class HDPutTransientOperation extends HDBasePutOperation {

    public HDPutTransientOperation() {
    }

    public HDPutTransientOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    protected void runInternal() {
        recordStore.putTransient(dataKey, dataValue, ttl);
        putTransient = true;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

}
