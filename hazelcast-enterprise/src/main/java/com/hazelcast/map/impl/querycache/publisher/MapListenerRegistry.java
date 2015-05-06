package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds {@link QueryCacheListenerRegistry} for each {@code IMap}.
 *
 * @see QueryCacheListenerRegistry
 */
public class MapListenerRegistry implements Registry<String, QueryCacheListenerRegistry> {

    private ConstructorFunction<String, QueryCacheListenerRegistry> registryConstructorFunction =
            new ConstructorFunction<String, QueryCacheListenerRegistry>() {
                @Override
                public QueryCacheListenerRegistry createNew(String mapName) {
                    return new QueryCacheListenerRegistry(context, mapName);
                }
            };

    private final QueryCacheContext context;
    private final ConcurrentMap<String, QueryCacheListenerRegistry> listeners;

    public MapListenerRegistry(QueryCacheContext context) {
        this.listeners = new ConcurrentHashMap<String, QueryCacheListenerRegistry>();
        this.context = context;
    }

    @Override
    public QueryCacheListenerRegistry getOrCreate(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(listeners, mapName, registryConstructorFunction);
    }

    @Override
    public QueryCacheListenerRegistry getOrNull(String mapName) {
        return listeners.get(mapName);
    }

    @Override
    public Map<String, QueryCacheListenerRegistry> getAll() {
        return Collections.unmodifiableMap(listeners);
    }

    @Override
    public QueryCacheListenerRegistry remove(String id) {
        return listeners.remove(id);
    }
}

