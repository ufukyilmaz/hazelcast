package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Used to register and hold {@link SubscriberRegistry} per {@link com.hazelcast.core.IMap IMap}
 *
 * @see SubscriberRegistry
 */
public class MapSubscriberRegistry implements Registry<String, SubscriberRegistry> {

    private ConstructorFunction<String, SubscriberRegistry> registryConstructorFunction =
            new ConstructorFunction<String, SubscriberRegistry>() {
                @Override
                public SubscriberRegistry createNew(String mapName) {
                    return createSubscriberRegistry(mapName);
                }
            };

    private final QueryCacheContext context;
    private final ConcurrentMap<String, SubscriberRegistry> cachePublishersPerIMap;

    public MapSubscriberRegistry(QueryCacheContext context) {
        this.context = context;
        this.cachePublishersPerIMap = new ConcurrentHashMap<String, SubscriberRegistry>();
    }

    @Override
    public SubscriberRegistry getOrCreate(String mapName) {
        return getOrPutIfAbsent(cachePublishersPerIMap, mapName, registryConstructorFunction);
    }

    @Override
    public SubscriberRegistry getOrNull(String mapName) {
        return cachePublishersPerIMap.get(mapName);
    }

    @Override
    public Map<String, SubscriberRegistry> getAll() {
        return Collections.unmodifiableMap(cachePublishersPerIMap);
    }

    @Override
    public SubscriberRegistry remove(String mapName) {
        return cachePublishersPerIMap.remove(mapName);
    }

    protected SubscriberRegistry createSubscriberRegistry(String mapName) {
        return new SubscriberRegistry(context, mapName);
    }

    protected QueryCacheContext getContext() {
        return context;
    }
}
