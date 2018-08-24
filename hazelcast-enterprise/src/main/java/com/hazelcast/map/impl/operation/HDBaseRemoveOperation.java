package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        // RU_COMPAT_3_10
        if (disableWanReplicationEvent && out.getVersion().isEqualTo(Versions.V3_10)) {

            // disableWanReplicationEvent flag is not serialized in 3.10, which may
            // lead to publishing remove WAN events by error, if the operation
            // executes on a remote node. This may lead to republishing remove
            // events to clusters that have already processed it, possibly causing
            // data loss, if the removed entry has been added back since then.
            //
            // Serializing the field would break the compatibility, hence
            // we encode its value into the TTL field, which is serialized
            // but not used for remove operations.
            //
            // Note that this serialization has the side effect that the
            // value of TTL changes, but it is acceptable since the field
            // is not in use.
            // This value change is done during serialization to keep
            // clusters already on 3.11+ unaffected from this compatibility trick.
            this.ttl ^= BITMASK_TTL_DISABLE_WAN;
        }

        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // RU_COMPAT_3_10
        if (in.getVersion().isEqualTo(Versions.V3_10)) {
            // restore disableWanReplicationEvent flag
            //
            // this may happen if the operation was created by a 3.10.5+ member
            // which carries over the disableWanReplicationEvent flag's value
            // in the TTL field for wire format compatibility reasons
            disableWanReplicationEvent |= (ttl & BITMASK_TTL_DISABLE_WAN) == 0;
        }
    }
}
