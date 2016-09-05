/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;

/**
 * Test publishing of near-cache invalidation events, when the cache is configured with NATIVE in-memory format.
 */
public class HiDensityClientNearCacheInvalidationTest extends ClientNearCacheInvalidationTest {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        return config;
    }

    @Override
    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = super.createClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);
        clientConfig.setNativeMemoryConfig(new NativeMemoryConfig().setSize(CLIENT_NATIVE_MEMORY_SIZE).setEnabled(true));
        return clientConfig;
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return super.createNearCacheConfig(inMemoryFormat);
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = super.createCacheConfig(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }
}
