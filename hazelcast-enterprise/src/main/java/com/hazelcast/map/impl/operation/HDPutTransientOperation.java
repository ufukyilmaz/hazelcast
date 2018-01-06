package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDPutTransientOperation extends HDBasePutOperation implements MutatingOperation {

    public HDPutTransientOperation() {
    }

    public HDPutTransientOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    protected void runInternal() {
        dataOldValue = mapServiceContext.toData(recordStore.putTransient(dataKey, dataValue, ttl));
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

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_TRANSIENT;
    }

}
