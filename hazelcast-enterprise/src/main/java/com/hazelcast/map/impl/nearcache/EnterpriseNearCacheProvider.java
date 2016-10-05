/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;

/**
 * Provides Near Cache specific functionality.
 */
public class EnterpriseNearCacheProvider extends NearCacheProvider {

    private final NearCacheContext nearCacheContext;

    public EnterpriseNearCacheProvider(MapServiceContext mapServiceContext) {
        super(mapServiceContext, new HDMapNearCacheManager(
                mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount()));

        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        NearCacheExecutor executor = new NearCacheExecutor() {
            @Override
            public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                return nodeEngine.getExecutionService().scheduleWithRepetition(command, initialDelay, delay, unit);
            }
        };

        this.nearCacheContext = new NearCacheContext(
                nodeEngine.getSerializationService(),
                executor,
                nearCacheManager,
                nodeEngine.getConfigClassLoader());
    }

    @Override
    public <K, V> NearCache<K, V> getOrCreateNearCache(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            NearCache<K, V> nearCache = nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);

            int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
            return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
        } else {
            return super.getOrCreateNearCache(mapName);
        }
    }

    private NearCacheConfig getNearCacheConfig(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig mapConfig = mapContainer.getMapConfig();

        return mapConfig.getNearCacheConfig();
    }
}
