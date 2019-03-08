package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDPutIfAbsentOperation extends HDBasePutOperation implements MutatingOperation {

    private boolean successful;

    public HDPutIfAbsentOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    public HDPutIfAbsentOperation() {
    }

    @Override
    protected void runInternal() {
        final Object oldValue = recordStore.putIfAbsent(dataKey, dataValue, ttl, maxIdle, getCallerAddress());
        this.oldValue = mapService.getMapServiceContext().toData(oldValue);
        successful = this.oldValue == null;
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
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_IF_ABSENT;
    }
}
