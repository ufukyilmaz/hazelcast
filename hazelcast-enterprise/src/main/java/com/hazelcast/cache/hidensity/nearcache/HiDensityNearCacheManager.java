package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.config.NearCacheConfig;

/**
 * {@link com.hazelcast.cache.impl.nearcache.NearCacheManager} implementation for Hi-Density cache.
 */
public class HiDensityNearCacheManager extends DefaultNearCacheManager {

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        return new HiDensityNearCache<K, V>(name, nearCacheConfig, nearCacheContext);
    }
}
