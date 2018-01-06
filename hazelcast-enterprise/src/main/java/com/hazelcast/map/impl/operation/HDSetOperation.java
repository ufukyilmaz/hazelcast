package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDSetOperation extends HDBasePutOperation implements IdentifiedDataSerializable, MutatingOperation {

    private boolean newRecord;

    public HDSetOperation() {
    }

    public HDSetOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        eventType = newRecord ? EntryEventType.ADDED : EntryEventType.UPDATED;
        super.afterRun();
    }

    @Override
    protected void runInternal() {
        newRecord = recordStore.set(dataKey, dataValue, ttl);
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
