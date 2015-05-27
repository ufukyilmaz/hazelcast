package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventDataBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for sweeping all registered accumulators of a {@link PublisherContext}.
 * This is needed in situations like ownership changes/graceful shutdown.
 */
public final class AccumulatorSweeper {

    private AccumulatorSweeper() {

    }

    public static void flushAllAccumulators(PublisherContext publisherContext) {
        QueryCacheContext context = publisherContext.getContext();
        EventPublisherAccumulatorProcessor processor
                = new EventPublisherAccumulatorProcessor(context.getQueryCacheEventService());
        PublisherAccumulatorHandler handler = new PublisherAccumulatorHandler(context, processor);

        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                for (Map.Entry<Integer, Accumulator> entry : accumulatorMap.entrySet()) {
                    Integer partitionId = entry.getKey();
                    Accumulator accumulator = entry.getValue();

                    processor.setInfo(accumulator.getInfo());

                    // Give 0 to delay-time in order to fetch all events in the accumulator.
                    accumulator.poll(handler, 0, TimeUnit.SECONDS);

                    // send end event
                    SingleEventData eventData = createEndOfSequenceEvent(partitionId);
                    processor.process(eventData);
                }
            }
        }
    }


    public static void flushAccumulator(PublisherContext publisherContext, int partitionId) {
        QueryCacheContext context = publisherContext.getContext();
        EventPublisherAccumulatorProcessor processor
                = new EventPublisherAccumulatorProcessor(context.getQueryCacheEventService());
        PublisherAccumulatorHandler handler = new PublisherAccumulatorHandler(context, processor);

        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                Accumulator accumulator = accumulatorMap.get(partitionId);
                if (accumulator == null) {
                    continue;
                }

                processor.setInfo(accumulator.getInfo());

                // Give 0 to delay-time in order to fetch all events in the accumulator.
                accumulator.poll(handler, 0, TimeUnit.SECONDS);
                // send end event
                SingleEventData eventData = createEndOfSequenceEvent(partitionId);
                processor.process(eventData);
           }
        }

    }


    public static void removeAccumulator(PublisherContext publisherContext, int partitionId) {
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                accumulatorRegistry.remove(partitionId);
            }
        }
    }


    /**
     * In graceful shutdown, we are flushing all unsent events in an {@code Accumulator}. This event
     * will be the last event of an {@code Accumulator} upon flush and it is used to inform subscriber-side
     * to state that we reached the end of event sequence for the relevant partition.
     * <p/>
     * After this event received by subscriber-side, subscriber resets its next-expected-sequence counter to zero for the
     * corresponding partition.
     */
    private static SingleEventData createEndOfSequenceEvent(int partitionId) {
        return SingleEventDataBuilder.newSingleEventDataBuilder().withSequence(-1).withPartitionId(partitionId).build();
    }

}
