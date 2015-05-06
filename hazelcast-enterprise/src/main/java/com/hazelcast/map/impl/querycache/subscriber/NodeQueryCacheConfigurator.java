package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;

import java.util.Iterator;
import java.util.List;

/**
 * Node side implementation of {@link QueryCacheConfigurator}
 *
 * @see QueryCacheConfigurator
 */
public class NodeQueryCacheConfigurator extends AbstractQueryCacheConfigurator {

    private final Config config;

    public NodeQueryCacheConfigurator(Config config, ClassLoader configClassLoader,
                                      QueryCacheEventService eventService) {
        super(configClassLoader, eventService);
        this.config = config;
    }

    @Override
    public QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);

        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        Iterator<QueryCacheConfig> iterator = queryCacheConfigs.iterator();
        while (iterator.hasNext()) {
            QueryCacheConfig config = iterator.next();
            if (config.getName().equals(cacheName)) {
                setPredicateImpl(config);
                setEntryListener(mapName, cacheName, config);
                return config;
            }
        }
        QueryCacheConfig newConfig = new QueryCacheConfig(cacheName);
        queryCacheConfigs.add(newConfig);
        return newConfig;
    }

    @Override
    public QueryCacheConfig getOrNull(String mapName, String cacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);

        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        Iterator<QueryCacheConfig> iterator = queryCacheConfigs.iterator();
        while (iterator.hasNext()) {
            QueryCacheConfig config = iterator.next();
            if (config.getName().equals(cacheName)) {
                return config;
            }
        }
        return null;
    }

    @Override
    public void removeConfiguration(String mapName, String cacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        if (queryCacheConfigs == null || queryCacheConfigs.isEmpty()) {
            return;
        }
        Iterator<QueryCacheConfig> iterator = queryCacheConfigs.iterator();
        while (iterator.hasNext()) {
            QueryCacheConfig config = iterator.next();
            if (config.getName().equals(cacheName)) {
                iterator.remove();
            }
        }
    }
}
