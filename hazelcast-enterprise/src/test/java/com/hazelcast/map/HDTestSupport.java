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

package com.hazelcast.map;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;

import javax.cache.CacheManager;

/**
 * Support class to provide hd specific configuration for map tests
 */
public final class HDTestSupport {

    public static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    public static Config getHDConfig() {
        return getHDConfig(new Config());
    }

    public static Config getHDConfig(Config config) {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(NATIVE_MEMORY_SIZE)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        return config
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    public static <K, V> IEnterpriseMap<K, V> getEnterpriseMap(HazelcastInstance instance, String mapName) {
        return (IEnterpriseMap<K, V>) instance.<K, V>getMap(mapName);
    }

    public static <K, V> ICache<K, V> getICache(CacheManager manager, CacheConfiguration<K, V> config, String cacheName) {
        return (ICache<K, V>) manager.createCache(cacheName, config);
    }
}
