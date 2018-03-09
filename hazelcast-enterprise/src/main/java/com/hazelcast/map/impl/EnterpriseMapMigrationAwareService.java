package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.EnterpriseMapReplicationOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;

import java.util.Collection;

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

        Operation operation = new EnterpriseMapReplicationOperation(container, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);

        Operation operation = new EnterpriseMapReplicationOperation(container, namespaces, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }
}
