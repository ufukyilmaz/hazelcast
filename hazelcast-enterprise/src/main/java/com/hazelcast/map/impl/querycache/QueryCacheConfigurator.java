package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.QueryCacheConfig;

/**
 * {@link QueryCacheConfig} supplier abstraction.
 * Helps to provide different implementations on node or client subscriber sides.
 */
public interface QueryCacheConfigurator {

    /**
     * Returns {@link QueryCacheConfig} for the requested query cache with cacheName
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     * @return {@link QueryCacheConfig} for the requested #cacheName.
     */
    QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName);


    /**
     * Returns {@link QueryCacheConfig} for the requested query cache with cacheName or null.
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     * @return {@link QueryCacheConfig} for the requested #cacheName.
     */
    QueryCacheConfig getOrNull(String mapName, String cacheName);

    /**
     * Removes corresponding configuration for the supplied cache name.
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     */
    void removeConfiguration(String mapName, String cacheName);
}
