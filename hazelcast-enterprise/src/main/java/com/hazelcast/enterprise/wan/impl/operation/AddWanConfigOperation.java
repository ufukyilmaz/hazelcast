package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Operation to add a new {@link WanReplicationConfig} at runtime.
 * This operation should be run on partition threads to achieve ordering
 * with other partition operations (map and cache mutation).
 * The operation also sends out backup operations to ensure we cover members
 * which are backups for any partitions for which they are replicas.
 */
public class AddWanConfigOperation extends Operation
        implements IdentifiedDataSerializable, BackupAwareOperation, Versioned {

    private WanReplicationConfig wanReplicationConfig;
    private boolean shouldBackup = true;

    @SuppressWarnings("unused")
    public AddWanConfigOperation() {
    }

    public AddWanConfigOperation(WanReplicationConfig wanReplicationConfig, boolean shouldBackup) {
        this.wanReplicationConfig = wanReplicationConfig;
        this.shouldBackup = shouldBackup;
    }

    @Override
    public void run() throws Exception {
        getNodeEngine().getWanReplicationService()
                       .addWanReplicationConfigLocally(wanReplicationConfig);
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        // all replicas should be notified in case some member contains only
        // backup replicas with a high replica count
        return IPartition.MAX_BACKUP_COUNT;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new AddWanConfigBackupOperation(wanReplicationConfig);
    }

    @Override
    public String getServiceName() {
        // the service name is null since the OperationBackupHandler.getBackupOperation
        // expects the backup operation to be namespace aware if the service is
        // namespace aware.
        // In this case, we don't have a specific namespace and we don't care about
        // namespaces. We just want to execute this operation on the partition thread.
        return null;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.ADD_WAN_CONFIG_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(wanReplicationConfig);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeBoolean(shouldBackup);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationConfig = in.readObject();
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            shouldBackup = in.readBoolean();
        }
    }
}
