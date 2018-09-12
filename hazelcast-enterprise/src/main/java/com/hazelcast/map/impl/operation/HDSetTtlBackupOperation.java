package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

public class HDSetTtlBackupOperation extends HDKeyBasedMapOperation {

    public HDSetTtlBackupOperation() {

    }

    public HDSetTtlBackupOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl, -1);
    }

    @Override
    protected void runInternal() {
        recordStore.setTtl(dataKey, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            publishWanUpdate(dataKey, record.getValue());
        }

        super.afterRun();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.SET_TTL_BACKUP;
    }
}
