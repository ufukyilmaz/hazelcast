package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import static com.hazelcast.core.EntryEventType.REMOVED;

public abstract class HDBaseRemoveOperation extends HDLockAwareOperation implements BackupAwareOperation {
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long BITMASK_TTL_DISABLE_WAN = 1L << 63;

    protected transient Data dataOldValue;

    public HDBaseRemoveOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public HDBaseRemoveOperation(String name, Data dataKey) {
        this(name, dataKey, false);
    }

    public HDBaseRemoveOperation() {
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        // RU_COMPAT_3_10
        //
        // we restore both the disableWanReplicationEvent and the ttl flags
        // before the operation is getting executed
        //
        // this may happen if the operation was created by a 3.10.5+ member
        // which carries over the disableWanReplicationEvent flag's value
        // in the TTL field for wire format compatibility reasons
        if ((ttl & BITMASK_TTL_DISABLE_WAN) == 0) {
            disableWanReplicationEvent = true;
            ttl ^= BITMASK_TTL_DISABLE_WAN;
        }
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterRemove(name, dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, REMOVED, dataKey, dataOldValue, null);
        invalidateNearCache(dataKey);
        publishWanRemove(dataKey);
        evict(dataKey);
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new HDRemoveBackupOperation(name, dataKey, false, disableWanReplicationEvent);
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

}
