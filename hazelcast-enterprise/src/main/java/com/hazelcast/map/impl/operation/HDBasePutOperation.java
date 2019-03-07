package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

public abstract class HDBasePutOperation extends HDLockAwareOperation implements BackupAwareOperation {

    protected transient Object oldValue;
    protected transient Data dataMergingValue;
    protected transient EntryEventType eventType;
    protected transient boolean putTransient;

    public HDBasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value, -1, -1);
    }

    public HDBasePutOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    public HDBasePutOperation() {
    }

    @Override
    public void afterRun() throws Exception {
        mapServiceContext.interceptAfterPut(name, dataValue);
        Object value = isPostProcessing(recordStore) ? recordStore.getRecord(dataKey).getValue() : dataValue;
        mapEventPublisher.publishEvent(getCallerAddress(), name, getEventType(),
                dataKey, oldValue, value, dataMergingValue);
        invalidateNearCache(dataKey);
        publishWanUpdate(dataKey, value);
        evict(dataKey);
    }

    private EntryEventType getEventType() {
        if (eventType == null) {
            eventType = oldValue == null ? ADDED : UPDATED;
        }
        return eventType;
    }

    @Override
    public boolean shouldBackup() {
        Record record = recordStore.getRecord(dataKey);
        return (record != null);
    }

    @Override
    public Operation getBackupOperation() {
        final Record record = recordStore.getRecord(dataKey);
        final RecordInfo replicationInfo = buildRecordInfo(record);
        if (isPostProcessing(recordStore)) {
            dataValue = mapServiceContext.toData(record.getValue());
        }
        return new HDPutBackupOperation(name, dataKey, dataValue, replicationInfo, shouldUnlockKeyOnBackup(),
                putTransient, !canThisOpGenerateWANEvent());
    }

    protected boolean shouldUnlockKeyOnBackup() {
        return false;
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
