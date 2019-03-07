package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDSetTtlOperation extends HDLockAwareOperation implements BackupAwareOperation, MutatingOperation {

    private transient boolean response;

    public HDSetTtlOperation() {

    }

    public HDSetTtlOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl, -1);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    protected void runInternal() {
        response = recordStore.setTtl(dataKey, ttl);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public void afterRun() throws Exception {
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            publishWanUpdate(dataKey, record.getValue());
            invalidateNearCache(dataKey);
        }

        super.afterRun();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.SET_TTL;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new HDSetTtlBackupOperation(name, dataKey, ttl);
    }
}