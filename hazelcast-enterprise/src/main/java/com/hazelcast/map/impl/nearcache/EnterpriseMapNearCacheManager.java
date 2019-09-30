package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.HiDensityNearCache;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Provides Near Cache specific functionality.
 */
public class EnterpriseMapNearCacheManager extends MapNearCacheManager {

    public EnterpriseMapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            EnterpriseSerializationService ess = (EnterpriseSerializationService) serializationService;
            HazelcastProperties properties = nodeEngine.getProperties();
            return new HiDensityNearCache<>(name, nearCacheConfig,
                    this, ess, scheduler, classLoader, properties);
        }
        return super.createNearCache(name, nearCacheConfig);
    }
}
