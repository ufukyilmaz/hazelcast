package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.NodeEngine;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;

/**
 * Provides Near Cache specific functionality.
 */
public class EnterpriseNearCacheProvider extends NearCacheProvider {

    public EnterpriseNearCacheProvider(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        super(new NearCacheContext(
                nodeEngine.getSerializationService(),
                nodeEngine.getExecutionService(),
                new HDMapNearCacheManager(nodeEngine.getPartitionService().getPartitionCount()),
                nodeEngine.getConfigClassLoader()
        ), mapServiceContext);
    }

    @Override
    public <K, V> NearCache<K, V> getOrCreateNearCache(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != NATIVE) {
            return super.getOrCreateNearCache(mapName);
        }

        NearCache<K, V> nearCache = nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);

        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
    }

    private NearCacheConfig getNearCacheConfig(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig mapConfig = mapContainer.getMapConfig();

        return mapConfig.getNearCacheConfig();
    }
}
