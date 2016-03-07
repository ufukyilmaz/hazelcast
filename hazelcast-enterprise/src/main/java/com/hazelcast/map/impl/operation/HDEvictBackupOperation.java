package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;

public class HDEvictBackupOperation extends HDKeyBasedMapOperation implements BackupOperation, MutatingOperation,
        IdentifiedDataSerializable {

    protected boolean unlockKey;
    protected boolean disableWanReplicationEvent;

    public HDEvictBackupOperation() {
    }

    public HDEvictBackupOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public HDEvictBackupOperation(String name, Data dataKey, boolean unlockKey) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
    }

    public HDEvictBackupOperation(String name, Data dataKey, boolean unlockKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    protected void runInternal() {
        recordStore.evict(dataKey, true);
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (!disableWanReplicationEvent
                && mapContainer.isWanReplicationEnabled()) {
            mapService.getMapServiceContext()
                    .getMapEventPublisher().publishWanReplicationRemoveBackup(name, dataKey, Clock.currentTimeMillis());
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.EVICT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(unlockKey);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        unlockKey = in.readBoolean();
        disableWanReplicationEvent = in.readBoolean();
    }

}
