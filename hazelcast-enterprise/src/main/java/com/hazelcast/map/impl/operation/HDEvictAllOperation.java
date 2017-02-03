package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

/**
 * Operation which evicts all keys except locked ones.
 */
public class HDEvictAllOperation extends HDMapOperation implements BackupAwareOperation,
        MutatingOperation, PartitionAwareOperation {

    private boolean shouldRunOnBackup;

    private int numberOfEvictedEntries;

    public HDEvictAllOperation() {
        this(null);
    }

    public HDEvictAllOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore != null) {
            numberOfEvictedEntries = recordStore.evictAll(false);
            shouldRunOnBackup = true;
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        invalidateAllKeysInNearCaches();
        hintMapEvent();
    }

    private void hintMapEvent() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.hintMapEvent(getCallerAddress(), name,
                EntryEventType.EVICT_ALL, numberOfEvictedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldRunOnBackup;
    }

    @Override
    public Object getResponse() {
        return numberOfEvictedEntries;
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
    public Operation getBackupOperation() {
        return new HDEvictAllBackupOperation(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(numberOfEvictedEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        numberOfEvictedEntries = in.readInt();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.EVICT_ALL;
    }
}
