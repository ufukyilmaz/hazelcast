package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDSetTTLOperation extends HDLockAwareOperation implements BackupAwareOperation, MutatingOperation {

    public HDSetTTLOperation() {

    }

    public HDSetTTLOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    protected void runInternal() {
        recordStore.setTTL(dataKey, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }
        if (mapContainer.isWanReplicationEnabled()) {
            EntryView entryView = EntryViews.createSimpleEntryView(dataKey, mapServiceContext.toData(record.getValue()), record);
            mapEventPublisher.publishWanUpdate(name, entryView);
        }
        invalidateNearCache(dataKey);
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
        return new HDSetTTLBackupOperation(name, dataKey, ttl);
    }
}
