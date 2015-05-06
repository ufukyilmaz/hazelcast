package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.core.IFunction;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds id's of registered listeners which are registered to listen underlying
 * {@code IMap} events to feed {@link com.hazelcast.map.QueryCache QueryCache}.
 * <p/>
 * This class contains mappings like : cacheName ---> registered listener ids for underlying {@code IMap}.
 */
public class QueryCacheListenerRegistry implements Registry<String, String> {

    private ConstructorFunction<String, String> registryConstructorFunction =
            new ConstructorFunction<String, String>() {
                @Override
                public String createNew(String ignored) {
                    IFunction<String, String> registration = context.getPublisherContext().getListenerRegistrator();
                    return registration.apply(mapName);
                }
            };

    private final String mapName;
    private final QueryCacheContext context;
    private final ConcurrentMap<String, String> listeners;

    public QueryCacheListenerRegistry(QueryCacheContext context, String mapName) {
        this.context = context;
        this.mapName = mapName;
        this.listeners = new ConcurrentHashMap<String, String>();
    }

    @Override
    public String getOrCreate(String cacheName) {
        return ConcurrencyUtil.getOrPutIfAbsent(listeners, cacheName, registryConstructorFunction);
    }

    @Override
    public String getOrNull(String cacheName) {
        return listeners.get(cacheName);
    }

    @Override
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(listeners);
    }

    @Override
    public String remove(String cacheName) {
        return listeners.remove(cacheName);
    }
}
