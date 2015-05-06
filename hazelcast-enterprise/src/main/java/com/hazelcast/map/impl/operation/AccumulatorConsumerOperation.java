package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorHandler;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Processes remaining items in {@link Accumulator} instances on a partition.
 * If the partition is not owned by this node, {@link Accumulator} will be removed.
 */
public class AccumulatorConsumerOperation extends AbstractOperation implements PartitionAwareOperation {

    private int maxProcessableAccumulatorCount;
    private Queue<Accumulator> accumulators;

    public AccumulatorConsumerOperation() {
    }

    public AccumulatorConsumerOperation(Queue<Accumulator> accumulators, int maxProcessableAccumulatorCount) {
        checkPositive(maxProcessableAccumulatorCount, "maxProcessableAccumulatorCount");

        this.accumulators = accumulators;
        this.maxProcessableAccumulatorCount = maxProcessableAccumulatorCount;
    }

    @Override
    public void run() throws Exception {
        QueryCacheContext context = getQueryCacheContext();

        QueryCacheEventService queryCacheEventService = context.getQueryCacheEventService();
        EventPublisherAccumulatorProcessor processor = new EventPublisherAccumulatorProcessor(queryCacheEventService);
        AccumulatorHandler<Sequenced> handler = new PublisherAccumulatorHandler(context, processor);

        int processed = 0;
        do {
            Accumulator accumulator = accumulators.poll();
            if (accumulator == null) {
                break;
            }

            if (isLocal()) {
                // consume the accumulator if only this node is the owner
                // of accumulators partition.
                publishAccumulator(processor, handler, accumulator);
            } else {
                // if the accumulator is not local, it should be a leftover
                // stayed after partition migrations and remove that accumulator.
                removeAccumulator(context, accumulator);
            }
            processed++;

        } while (processed <= maxProcessableAccumulatorCount);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    private void publishAccumulator(EventPublisherAccumulatorProcessor processor,
                                    AccumulatorHandler<Sequenced> handler, Accumulator accumulator) {
        AccumulatorInfo info = accumulator.getInfo();
        processor.setInfo(info);
        accumulator.poll(handler, info.getDelaySeconds(), TimeUnit.SECONDS);
    }

    private QueryCacheContext getQueryCacheContext() {
        MapService mapService = (MapService) getService();
        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }

    private boolean isLocal() {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        InternalPartition partition = partitionService.getPartition(getPartitionId());
        return partition.isLocal();
    }

    private void removeAccumulator(QueryCacheContext context, Accumulator accumulator) {
        PublisherContext publisherContext = context.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        AccumulatorInfo info = accumulator.getInfo();
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();

        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return;
        }

        PartitionAccumulatorRegistry partitionAccumulatorRegistry = publisherRegistry.getOrNull(cacheName);
        if (partitionAccumulatorRegistry == null) {
            return;
        }

        partitionAccumulatorRegistry.remove(getPartitionId());
    }
}
