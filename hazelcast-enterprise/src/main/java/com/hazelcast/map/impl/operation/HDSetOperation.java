package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;

public class HDSetOperation extends HDBasePutOperation implements IdentifiedDataSerializable {

    private boolean newRecord;

    public HDSetOperation() {
    }

    public HDSetOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    protected void runInternal() {
        Object oldValue = recordStore.set(dataKey, dataValue, ttl);
        newRecord = oldValue == null;

        if (recordStore.hasQueryCache()) {
            dataOldValue = mapServiceContext.toData(oldValue);
        }
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
