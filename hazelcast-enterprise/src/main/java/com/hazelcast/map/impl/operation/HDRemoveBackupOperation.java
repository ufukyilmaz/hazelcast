package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;

public class HDRemoveBackupOperation extends HDKeyBasedMapOperation implements BackupOperation,
        IdentifiedDataSerializable {

    protected boolean unlockKey;
    protected boolean disableWanReplicationEvent;

    public HDRemoveBackupOperation() {
    }

    public HDRemoveBackupOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public HDRemoveBackupOperation(String name, Data dataKey, boolean unlockKey) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
    }

    public HDRemoveBackupOperation(String name, Data dataKey, boolean unlockKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    protected void runInternal() {
        recordStore.removeBackup(dataKey);
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (!disableWanReplicationEvent
                && mapContainer.isWanReplicationEnabled()) {
            mapEventPublisher.publishWanReplicationRemoveBackup(name, dataKey, Clock.currentTimeMillis());
        }
        evict(dataKey);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.REMOVE_BACKUP;
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
