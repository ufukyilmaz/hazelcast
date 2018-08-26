package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.REMOVED;

public abstract class HDBaseRemoveOperation extends HDLockAwareOperation implements BackupAwareOperation {
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long BITMASK_TTL_DISABLE_WAN = 1L << 63;

    protected transient Data dataOldValue;

    /**
     * Used by WAN replication service to disable WAN replication event publishing.
     * Otherwise, in active-active scenarios infinite loop of event forwarding can be seen.
     */
    protected boolean disableWanReplicationEvent;

    public HDBaseRemoveOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.disableWanReplicationEvent = disableWanReplicationEvent;

        // disableWanReplicationEvent flag is not serialized, which may
        // lead to publishing remove WAN events by error, if the operation
        // executes on a remote node. This may lead to republishing remove
        // events to clusters that have already processed it, possibly causing
        // data loss, if the removed entry has been added back since then.
        //
        // Serializing the field would break the compatibility, hence
        // we encode its value into the TTL field, which is serialized
        // but not used for remove operations.
        if (disableWanReplicationEvent) {
            this.ttl ^= BITMASK_TTL_DISABLE_WAN;
        }
    }

    public HDBaseRemoveOperation(String name, Data dataKey) {
        this(name, dataKey, false);
    }

    public HDBaseRemoveOperation() {
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterRemove(name, dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, REMOVED, dataKey, dataOldValue, null);
        invalidateNearCache(dataKey);
        if (mapContainer.isWanReplicationEnabled() && !disableWanReplicationEvent) {
            // TODO should evict operation be replicated?
            mapEventPublisher.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
        }
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

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // restore disableWanReplicationEvent flag
        disableWanReplicationEvent = (ttl & BITMASK_TTL_DISABLE_WAN) == 0;
    }
}
