package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.NearCacheConfig;

import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;

class HDMapNearCacheManager extends HiDensityNearCacheManager {

    private final int partitionCount;

    HDMapNearCacheManager(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        NearCache<K, V> nearCache = super.createNearCache(name, nearCacheConfig, nearCacheContext);
        return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
    }
}
