package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.asStaleReadPreventerNearCache;

/**
 * Provides Near Cache specific functionality.
 */
public class EnterpriseMapNearCacheManager extends MapNearCacheManager {

    public EnterpriseMapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        if (NATIVE == nearCacheConfig.getInMemoryFormat()) {
            HiDensityNearCache<K, V> nearCache = new HiDensityNearCache<K, V>(name, nearCacheConfig, this,
                    ((EnterpriseSerializationService) serializationService), executionService, classLoader);

            return asStaleReadPreventerNearCache(nearCache, partitionCount);
        }

        return super.createNearCache(name, nearCacheConfig);
    }

}
