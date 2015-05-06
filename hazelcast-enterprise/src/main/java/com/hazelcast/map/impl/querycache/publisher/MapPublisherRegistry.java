package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Registry of mappings like {@code mapName} to {@code PublisherRegistry}.
 *
 * @see PublisherRegistry
 */
public class MapPublisherRegistry implements Registry<String, PublisherRegistry> {

    private ConstructorFunction<String, PublisherRegistry> registryConstructorFunction =
            new ConstructorFunction<String, PublisherRegistry>() {
                @Override
                public PublisherRegistry createNew(String mapName) {
                    return createPublisherRegistry(mapName);
                }
            };

    private final QueryCacheContext context;
    private final ConcurrentMap<String, PublisherRegistry> cachePublishersPerIMap;

    public MapPublisherRegistry(QueryCacheContext context) {
        this.context = context;
        this.cachePublishersPerIMap = new ConcurrentHashMap<String, PublisherRegistry>();
    }

    @Override
    public PublisherRegistry getOrCreate(String mapName) {
        return getOrPutIfAbsent(cachePublishersPerIMap, mapName, registryConstructorFunction);
    }

    @Override
    public PublisherRegistry getOrNull(String mapName) {
        return cachePublishersPerIMap.get(mapName);
    }

    @Override
    public Map<String, PublisherRegistry> getAll() {
        return Collections.unmodifiableMap(cachePublishersPerIMap);
    }

    @Override
    public PublisherRegistry remove(String id) {
        return cachePublishersPerIMap.remove(id);
    }

    protected PublisherRegistry createPublisherRegistry(String mapName) {
        return new PublisherRegistry(context, mapName);
    }
}
