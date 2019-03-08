package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;

public class HDSetOperation extends HDBasePutOperation implements IdentifiedDataSerializable, MutatingOperation {

    private boolean newRecord;

    public HDSetOperation() {
    }

    public HDSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    @Override
    protected void runInternal() {
        oldValue = recordStore.set(dataKey, dataValue, ttl, maxIdle);
        newRecord = oldValue == null;
    }

    @Override
    public void afterRun() throws Exception {
        eventType = newRecord ? ADDED : UPDATED;
        super.afterRun();
    }

    @Override
    public Object getResponse() {
        return newRecord;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.SET;
    }

}
