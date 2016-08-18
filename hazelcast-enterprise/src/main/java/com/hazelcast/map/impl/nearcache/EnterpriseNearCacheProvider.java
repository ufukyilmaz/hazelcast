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
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Provides Near Cache specific functionality.
 */
public class EnterpriseNearCacheProvider extends NearCacheProvider {

    private final SerializationService serializationService;
    private final NearCacheExecutor executor;
    private final NearCacheManager nearCacheManager;
    private final NearCacheContext nearCacheContext;

    public EnterpriseNearCacheProvider(final MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.executor = new NearCacheExecutor() {

            @Override
            public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay,
                                                             long delay, TimeUnit unit) {
                return mapServiceContext.getNodeEngine().getExecutionService().scheduleWithRepetition(command, initialDelay,
                        delay, unit);
            }
        };
        this.nearCacheManager = new HDMapNearCacheManager(nodeEngine.getPartitionService().getPartitionCount());
        this.nearCacheContext =
                new NearCacheContext(nearCacheManager,
                        serializationService,
                        executor,
                        mapServiceContext.getNodeEngine().getConfigClassLoader());
    }

    @Override
    public NearCache getOrNullNearCache(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            return nearCacheManager.getNearCache(mapName);
        } else {
            return nearCacheMap.get(mapName);
        }
    }


    @Override
    public NearCache getOrCreateNearCache(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            return nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);
        } else {
            return super.getOrCreateNearCache(mapName);
        }
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getNearCacheConfig();
    }

    @Override
    public void destroyNearCache(String mapName) {
        super.destroyNearCache(mapName);
        nearCacheManager.destroyNearCache(mapName);
    }
}

