package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.HDMapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;

import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.flushAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.removeAccumulator;

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
        HDMapReplicationOperation operation
                = new HDMapReplicationOperation(event.getPartitionId(), event.getReplicaIndex(), mapServiceContext);
        operation.setService(mapServiceContext.getService());
        return operation;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        super.commitMigration(event);

        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int partitionId = event.getPartitionId();
            flushAccumulator(publisherContext, partitionId);
            removeAccumulator(publisherContext, partitionId);
        }
    }
}
