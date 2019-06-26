package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Backup operation to add a new {@link WanReplicationConfig} at runtime.
 * This operation should be run on partition threads to achieve ordering
 * with other partition operations (map and cache mutation).
 */
public class AddWanConfigBackupOperation extends Operation implements IdentifiedDataSerializable, BackupOperation {

    private WanReplicationConfig wanReplicationConfig;

    @SuppressWarnings("unused")
    public AddWanConfigBackupOperation() {
    }

    public AddWanConfigBackupOperation(WanReplicationConfig wanReplicationConfig) {
        this.wanReplicationConfig = wanReplicationConfig;
    }

    @Override
    public void run() throws Exception {
        getNodeEngine().getWanReplicationService()
                       .addWanReplicationConfigLocally(wanReplicationConfig);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(wanReplicationConfig);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationConfig = in.readObject();
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
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.ADD_WAN_CONFIG_BACKUP_OPERATION;
    }
}
