package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheManager} implementation for Hi-Density cache.
 */
public class HiDensityNearCacheManager extends DefaultNearCacheManager {

    public HiDensityNearCacheManager(EnterpriseSerializationService ss, TaskScheduler es,
                                     ClassLoader classLoader, HazelcastProperties properties) {
        super(ss, es, classLoader, properties);
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        return new HiDensityNearCache<>(name, nearCacheConfig, this,
                ((EnterpriseSerializationService) serializationService), scheduler, classLoader, properties);
    }
}
