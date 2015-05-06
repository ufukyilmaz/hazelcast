package com.hazelcast.map.impl;

import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorHandler;
import com.hazelcast.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Contains {@link com.hazelcast.map.QueryCache QueryCache} flush logic when system shutting down.
 */
public class EnterpriseMapManagedService extends MapManagedService {

    private final EnterpriseMapServiceContext mapServiceContext;

    EnterpriseMapManagedService(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void shutdown(boolean terminate) {
        super.shutdown(terminate);

        if (!terminate) {
            flushQueryCacheAccumulators();
        }
    }

    private void flushQueryCacheAccumulators() {
        QueryCacheContext context = mapServiceContext.getQueryCacheContext();
        QueryCacheEventService queryCacheEventService = context.getQueryCacheEventService();
        EventPublisherAccumulatorProcessor processor = new EventPublisherAccumulatorProcessor(queryCacheEventService);
        AccumulatorHandler<Sequenced> handler = new PublisherAccumulatorHandler(context, processor);

        PublisherContext publisherContext = context.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();
        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                for (Accumulator accumulator : accumulatorMap.values()) {
                    processor.setInfo(accumulator.getInfo());
                    // Give 0 to delay-time in order to fetch all events in the accumulator.
                    accumulator.poll(handler, 0, TimeUnit.SECONDS);
                }
            }
        }
    }

}
