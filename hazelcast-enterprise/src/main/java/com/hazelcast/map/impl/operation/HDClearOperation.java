package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class HDClearOperation extends HDMapOperation implements BackupAwareOperation,
        PartitionAwareOperation, MutatingOperation {

    boolean shouldBackup = true;

    private int numberOfClearedEntries;

    public HDClearOperation() {
        this(null);
    }

    public HDClearOperation(String name) {
        super(name);
        this.createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore != null) {
            numberOfClearedEntries = recordStore.clear();
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        invalidateAllKeysInNearCaches();
        hintMapEvent();

        disposeDeferredBlocks();
    }

    private void hintMapEvent() {
        mapEventPublisher.hintMapEvent(getCallerAddress(), name, EntryEventType.CLEAR_ALL,
                numberOfClearedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Object getResponse() {
        return numberOfClearedEntries;
    }

    public Operation getBackupOperation() {
        HDClearBackupOperation clearBackupOperation = new HDClearBackupOperation(name);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.CLEAR;
    }
}
