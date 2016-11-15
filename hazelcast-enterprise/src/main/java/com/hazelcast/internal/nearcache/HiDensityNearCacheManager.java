package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.ExecutionService;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheManager} implementation for Hi-Density cache.
 */
public class HiDensityNearCacheManager extends DefaultNearCacheManager {

    public HiDensityNearCacheManager(EnterpriseSerializationService ss, ExecutionService es, ClassLoader classLoader) {
        super(ss, es, classLoader);
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        return new HiDensityNearCache<K, V>(name, nearCacheConfig, this,
                ((EnterpriseSerializationService) serializationService), executionService, classLoader);
    }
}
