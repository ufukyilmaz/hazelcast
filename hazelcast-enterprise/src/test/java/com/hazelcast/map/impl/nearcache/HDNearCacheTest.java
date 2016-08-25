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

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.nearcache.NearCacheTest;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDNearCacheTest extends NearCacheTest {

    /**
     * HD backed near cache does not support NONE eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WitNone_whenMaxSizeExceeded() {
        testEvictionPolicyInternal(NONE);
    }

    /**
     * HD backed near cache does not support RANDOM eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() {
        testEvictionPolicyInternal(RANDOM);
    }

    private void testEvictionPolicyInternal(EvictionPolicy evictionPolicy) {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(evictionPolicy);

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEntryCountMaxSizePolicy_isNotSupportedByNearCache_whenInMemoryFormatIsNative() throws Exception {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setMaximumSizePolicy(ENTRY_COUNT);

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }

    @Test
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() {
        testNearCacheInvalidationInternal("LFU");
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() {
        testNearCacheInvalidationInternal("LRU");
    }

    private void testNearCacheInvalidationInternal(String evictionPolicy) {
        String mapName = randomMapName();
        final int putCount = 2000;

        NearCacheConfig nearCacheConfig = newNearCacheConfig().setInvalidateOnChange(false);
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.valueOf(evictionPolicy));

        Config config = newNativeMemoryConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        final IMap<Integer, Integer> map = createHazelcastInstance(config).getMap(mapName);
        pullEntriesToNearCache(map, putCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                long ownedEntryCount = stats.getOwnedEntryCount();
                triggerNearCacheEviction(map);
                assertTrue("owned entry count " + ownedEntryCount, putCount > ownedEntryCount);
            }
        });
    }

    @Override
    protected Config getConfig() {
        return newNativeMemoryConfig();
    }

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(90);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(false);
        return nearCacheConfig;
    }

    private static Config newNativeMemoryConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("default");
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig();
        memoryConfig.setEnabled(true);
        memoryConfig.setSize(new MemorySize(8, MemoryUnit.MEGABYTES));
        memoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        config.addMapConfig(mapConfig);
        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }
}
