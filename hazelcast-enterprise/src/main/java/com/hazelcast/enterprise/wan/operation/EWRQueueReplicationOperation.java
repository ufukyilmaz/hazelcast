package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

/**
 * Migration operation
 */
public class EWRQueueReplicationOperation extends AbstractOperation implements IdentifiedDataSerializable {

    private EWRMigrationContainer ewrMigrationContainer;

    public EWRQueueReplicationOperation() {

    }

    public EWRQueueReplicationOperation(EWRMigrationContainer ewrMigrationContainer) {
        this.ewrMigrationContainer = ewrMigrationContainer;
    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_QUEUE_REPLICATION_OPERATION;
    }

    @Override
    public String getServiceName() {
        return EnterpriseWanReplicationService.SERVICE_NAME;
    }
}
