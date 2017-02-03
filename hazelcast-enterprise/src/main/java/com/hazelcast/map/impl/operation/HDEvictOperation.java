package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDEvictOperation extends HDLockAwareOperation implements MutatingOperation, BackupAwareOperation {

    private boolean evicted;
    private boolean asyncBackup;

    public HDEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        super(name, dataKey);
        this.asyncBackup = asyncBackup;
    }

    public HDEvictOperation() {
    }

    @Override
    protected void runInternal() {
        dataValue = mapServiceContext.toData(recordStore.evict(dataKey, false));
        evicted = dataValue != null;
    }

    @Override
    public void afterRun() {
        if (!evicted) {
            return;
        }
        mapServiceContext.interceptAfterRemove(name, dataValue);
        EntryEventType eventType = EntryEventType.EVICTED;
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, dataValue, null);
        invalidateNearCache(dataKey);

        disposeDeferredBlocks();
    }

    @Override
    public Object getResponse() {
        return evicted;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    public Operation getBackupOperation() {
        return new HDEvictBackupOperation(name, dataKey);
    }

    @Override
    public int getAsyncBackupCount() {
        if (asyncBackup) {
            return mapContainer.getTotalBackupCount();
        }

        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        }
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return evicted;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(asyncBackup);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        asyncBackup = in.readBoolean();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.EVICT;
    }
}
