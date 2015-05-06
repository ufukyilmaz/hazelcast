package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorFactory;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Registry of mappings like {@code cacheName} to {@code PartitionAccumulatorRegistry}.
 *
 * @see PartitionAccumulatorRegistry
 */
public class PublisherRegistry implements Registry<String, PartitionAccumulatorRegistry> {

    private final ConstructorFunction<String, PartitionAccumulatorRegistry> partitionAccumulatorRegistryConstructor =
            new ConstructorFunction<String, PartitionAccumulatorRegistry>() {
                @Override
                public PartitionAccumulatorRegistry createNew(String cacheName) {
                    AccumulatorInfo info = getAccumulatorInfo(cacheName);
                    checkNotNull(info, "info cannot be null");

                    AccumulatorFactory accumulatorFactory = createPublisherAccumulatorFactory();
                    ConstructorFunction<Integer, Accumulator> constructor
                            = new PublisherAccumulatorConstructor(info, accumulatorFactory);
                    return new PartitionAccumulatorRegistry(info, constructor);
                }
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, PartitionAccumulatorRegistry> partitionAccumulators;

    public PublisherRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.partitionAccumulators = new ConcurrentHashMap<String, PartitionAccumulatorRegistry>();

    }

    @Override
    public PartitionAccumulatorRegistry getOrCreate(String cacheName) {
        return getOrPutIfAbsent(partitionAccumulators, cacheName, partitionAccumulatorRegistryConstructor);
    }

    @Override
    public PartitionAccumulatorRegistry getOrNull(String cacheName) {
        return partitionAccumulators.get(cacheName);
    }

    @Override
    public Map<String, PartitionAccumulatorRegistry> getAll() {
        return Collections.unmodifiableMap(partitionAccumulators);
    }

    @Override
    public PartitionAccumulatorRegistry remove(String cacheName) {
        return partitionAccumulators.remove(cacheName);
    }

    /**
     * Constructor used to create a publisher accumulator.
     */
    private static class PublisherAccumulatorConstructor implements ConstructorFunction<Integer, Accumulator> {

        private final AccumulatorInfo info;

        private final AccumulatorFactory accumulatorFactory;

        public PublisherAccumulatorConstructor(AccumulatorInfo info, AccumulatorFactory accumulatorFactory) {
            this.info = info;
            this.accumulatorFactory = accumulatorFactory;
        }

        @Override
        public Accumulator createNew(Integer partitionId) {
            return accumulatorFactory.createAccumulator(info);
        }

    }

    private AccumulatorInfo getAccumulatorInfo(String cacheName) {
        PublisherContext publisherContext = context.getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        return infoSupplier.getAccumulatorInfoOrNull(mapName, cacheName);
    }

    protected PublisherAccumulatorFactory createPublisherAccumulatorFactory() {
        return new PublisherAccumulatorFactory(context);
    }
}
