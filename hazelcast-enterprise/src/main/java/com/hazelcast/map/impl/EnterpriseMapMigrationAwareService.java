package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.EnterpriseMapReplicationOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;

/**
 * Defines enterprise only migration aware service behavior for {@link MapService}.
 *
 * @see MapService
 */
public class EnterpriseMapMigrationAwareService extends MapMigrationAwareService {

    private EnterpriseMapServiceContext mapServiceContext;

    public EnterpriseMapMigrationAwareService(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);

        EnterpriseMapReplicationOperation operation
                = new EnterpriseMapReplicationOperation(container, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());

        return operation;
    }
}
