//package com.hazelcast.client.cache;
//
//import com.hazelcast.cache.HazelcastCacheManager;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.HazelcastClientProxy;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.config.NearCacheConfig;
//
//import javax.cache.Cache;
//import javax.cache.CacheManager;
//import javax.cache.configuration.Configuration;
//
//final class HazelcastClientCacheManager extends HazelcastCacheManager implements CacheManager {
//
//    public HazelcastClientCacheManager(HazelcastClientCachingProvider cachingProvider) {
//        super(cachingProvider.hazelcastInstance, cachingProvider);
//    }
//
//    @Override
//    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
//            throws IllegalArgumentException {
//
//        if (configuration != null) {
//            if (configuration instanceof NearCacheConfig) {
//                NearCacheConfig cacheConfig = (NearCacheConfig) configuration;
//                ClientConfig clientConfig = null;
//                if (hazelcastInstance instanceof HazelcastClientProxy) {
//                    clientConfig = ((HazelcastClientProxy) hazelcastInstance).getClientConfig();
//                } else if (hazelcastInstance instanceof HazelcastClient) {
//                    clientConfig = ((HazelcastClient) hazelcastInstance).getClientConfig();
//                } else {
//                    throw new IllegalStateException("Unexpected HazelcastInstance!");
//                }
//                cacheConfig.setName(cacheName);
//                clientConfig.addNearCacheConfig(cacheName, cacheConfig);
//
//            } else {
//                throw new IllegalArgumentException("Unknown configuration: " + configuration);
//            }
//        }
//        return getCache(cacheName);
//    }
//}
