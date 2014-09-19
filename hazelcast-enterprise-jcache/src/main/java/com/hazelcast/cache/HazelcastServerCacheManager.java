package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;

final class HazelcastServerCacheManager extends HazelcastCacheManager implements CacheManager {

    HazelcastServerCacheManager(HazelcastServerCachingProvider cachingProvider) {
        super(cachingProvider.hazelcastInstance, cachingProvider);
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        if (configuration != null) {
            if (configuration instanceof CacheConfig) {
                CacheConfig cacheConfig = (CacheConfig) configuration;
                cacheConfig.setName(cacheName);
                hazelcastInstance.getConfig().addCacheConfig(cacheConfig);
            } else {
                throw new IllegalArgumentException("Unknown configuration: " + configuration);
            }
        }
        return getCache(cacheName);
    }
}
