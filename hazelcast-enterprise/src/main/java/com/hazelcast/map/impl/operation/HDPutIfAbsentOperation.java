package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;

public class HDPutIfAbsentOperation extends HDBasePutOperation {

    private boolean successful;

    public HDPutIfAbsentOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    public HDPutIfAbsentOperation() {
    }

    @Override
    protected void runInternal() {
        final Object oldValue = recordStore.putIfAbsent(dataKey, dataValue, ttl);
        dataOldValue = mapService.getMapServiceContext().toData(oldValue);
        successful = dataOldValue == null;
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
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_IF_ABSENT;
    }
}
