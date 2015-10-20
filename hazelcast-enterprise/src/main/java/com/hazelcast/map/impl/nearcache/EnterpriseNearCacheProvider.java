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

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Provides near cache specific functionality.
 */
public class EnterpriseNearCacheProvider extends NearCacheProvider {

    private final SerializationService serializationService;
    private final NearCacheExecutor executor;
    private final NearCacheManager nearCacheManager;
    private final NearCacheContext nearCacheContext;

    public EnterpriseNearCacheProvider(final MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        super(mapServiceContext, nodeEngine);
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.executor = new NearCacheExecutor() {

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                             long delay, TimeUnit unit) {
                return mapServiceContext.getNodeEngine().getExecutionService().scheduleWithFixedDelay(command, initialDelay,
                        delay, unit);
            }
        };
        this.nearCacheManager = new HiDensityNearCacheManager();
        this.nearCacheContext = new NearCacheContext(nearCacheManager, serializationService, executor);
    }


    @Override
    public NearCache getOrCreateNearCache(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig mapConfig = mapContainer.getMapConfig();
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            return nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);
        } else {
            return super.getOrCreateNearCache(mapName);
        }
    }

    @Override
    public void remove(String mapName) {
        super.remove(mapName);
        nearCacheManager.destroyNearCache(mapName);
    }
}

