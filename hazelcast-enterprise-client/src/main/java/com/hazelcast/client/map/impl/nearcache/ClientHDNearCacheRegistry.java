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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.utils.AbstractRegistry;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ClientHDNearCacheRegistry extends AbstractRegistry<String, NearCache> {

    private final NearCacheManager nearCacheManager;
    private final NearCacheContext nearCacheContext;
    private final ClientConfig clientConfig;

    public ClientHDNearCacheRegistry(ClientExecutionService executionService,
                                     SerializationService serializationService,
                                     ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.nearCacheManager = new HiDensityNearCacheManager();
        this.nearCacheContext = new NearCacheContext(nearCacheManager,
                serializationService,
                createNearCacheExecutor(executionService));
    }

    protected NearCacheExecutor createNearCacheExecutor(final ClientExecutionService executionService) {
        return new NearCacheExecutor() {

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                return executionService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
            }
        };
    }


    @Override
    public ConstructorFunction<String, NearCache> getConstructorFunction() {
        return new ConstructorFunction<String, NearCache>() {
            @Override
            public NearCache createNew(String mapName) {
                NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(mapName);
                return nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);
            }
        };
    }

    @Override
    public NearCache remove(String cacheName) {
        NearCache removed = super.remove(cacheName);
        if (removed != null) {
            nearCacheManager.destroyNearCache(removed.getName());
        }
        return removed;
    }
}
