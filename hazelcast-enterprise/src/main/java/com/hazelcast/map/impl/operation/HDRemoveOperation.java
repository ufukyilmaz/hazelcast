package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDRemoveOperation extends HDBaseRemoveOperation implements IdentifiedDataSerializable, MutatingOperation {

    protected boolean successful;

    public HDRemoveOperation() {
    }

    public HDRemoveOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey, disableWanReplicationEvent);
    }

    @Override
    protected void runInternal() {
        dataOldValue = mapServiceContext.toData(recordStore.remove(dataKey));
        successful = dataOldValue != null;
    }

    @Override
    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    @Override
    public boolean shouldBackup() {
        return successful;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.REMOVE;
    }
}
