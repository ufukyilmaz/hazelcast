package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.EnterpriseMapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;

/**
 * Defines enterprise only migration aware service behavior for {@link MapService}
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
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        EnterpriseMapReplicationOperation operation = createOperation(event, container, publisherContext);
        return operation.isEmpty() ? null : operation;
    }

    private EnterpriseMapReplicationOperation createOperation(PartitionReplicationEvent event,
                                                              PartitionContainer container,
                                                              PublisherContext publisherContext) {
        MapService service = mapServiceContext.getService();
        int partitionId = event.getPartitionId();
        int replicaIndex = event.getReplicaIndex();

        EnterpriseMapReplicationOperation operation = new EnterpriseMapReplicationOperation(service, container,
                partitionId, replicaIndex);
        operation.setService(service);
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }
}
