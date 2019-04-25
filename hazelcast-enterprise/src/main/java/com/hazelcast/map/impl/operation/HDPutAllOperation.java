package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

public class HDPutAllOperation extends HDMapOperation implements PartitionAwareOperation, BackupAwareOperation,
        MutatingOperation {

    private MapEntries mapEntries;
    private int currentIndex;

    private boolean hasMapListener;
    private boolean hasWanReplication;
    private boolean hasBackups;
    private boolean hasInvalidation;

    private List<RecordInfo> backupRecordInfos;
    private List<Data> invalidationKeys;

    @SuppressWarnings("unused")
    public HDPutAllOperation() {
    }

    public HDPutAllOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();

        this.hasMapListener = mapEventPublisher.hasEventListener(name);
        this.hasWanReplication = mapContainer.isWanReplicationEnabled();
        this.hasBackups = hasBackups();
        this.hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            this.backupRecordInfos = new ArrayList<>(mapEntries.size());
        }
        if (hasInvalidation) {
            this.invalidationKeys = new ArrayList<>(mapEntries.size());
        }
    }

    @Override
    protected void runInternal() {
        // if currentIndex is not zero, this is a continuation of the operation after a NativeOOME
        int size = mapEntries.size();
        while (currentIndex < size) {
            put(mapEntries.getKey(currentIndex), mapEntries.getValue(currentIndex));
            currentIndex++;
        }
    }

    private boolean hasBackups() {
        return (mapContainer.getTotalBackupCount() > 0);
    }

    private void put(Data dataKey, Data dataValue) {
        Object oldValue = putToRecordStore(dataKey, dataValue);
        dataValue = getValueOrPostProcessedValue(dataKey, dataValue);
        mapServiceContext.interceptAfterPut(name, dataValue);

        if (hasMapListener) {
            EntryEventType eventType = (oldValue == null ? ADDED : UPDATED);
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);
        }

        if (hasWanReplication) {
            publishWanUpdate(dataKey, dataValue);
        }
        if (hasBackups) {
            Record record = recordStore.getRecord(dataKey);
            RecordInfo replicationInfo = buildRecordInfo(record);
            backupRecordInfos.add(replicationInfo);
        }

        evict(dataKey);
        if (hasInvalidation) {
            invalidationKeys.add(dataKey);
        }
    }

    /**
     * The method recordStore.put() tries to fetch the old value from the MapStore,
     * which can lead to a serious performance degradation if loading from MapStore is expensive.
     * We prevent this by calling recordStore.set() if no map listeners are registered.
     */
    private Object putToRecordStore(Data dataKey, Data dataValue) {
        if (hasMapListener) {
            return recordStore.put(dataKey, dataValue, DEFAULT_TTL, DEFAULT_MAX_IDLE);
        }
        recordStore.set(dataKey, dataValue, DEFAULT_TTL, DEFAULT_MAX_IDLE);
        return null;
    }

    @Override
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);

        super.afterRun();
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!isPostProcessing(recordStore)) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return (hasBackups && !mapEntries.isEmpty());
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
    public Operation getBackupOperation() {
        return new HDPutAllBackupOperation(name, mapEntries, backupRecordInfos, false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_ALL;
    }
}
