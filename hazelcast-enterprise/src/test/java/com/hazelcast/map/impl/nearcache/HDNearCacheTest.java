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
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDNearCacheTest extends NearCacheTest {

    /**
     * HD backed Near Cache doesn't support NONE eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    @Override
    public void testNearCacheInvalidation_WitNone_whenMaxSizeExceeded() {
        testEvictionPolicyInternal(NONE);
    }

    /**
     * HD backed Near Cache doesn't support RANDOM eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    @Override
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

    @Test
    @Override
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() {
        testNearCacheInvalidationInternal("LFU");
    }

    @Test
    @Override
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() {
        testNearCacheInvalidationInternal("LRU");
    }

    private void testNearCacheInvalidationInternal(String evictionPolicy) {
        final int mapSize = 2000;
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = newNearCacheConfig().setInvalidateOnChange(false);
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.valueOf(evictionPolicy));

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        final IMap<Integer, Integer> map = createHazelcastInstance(config).getMap(mapName);
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long ownedEntryCount = getNearCacheStats(map).getOwnedEntryCount();
                triggerNearCacheEviction(map);
                assertTrue("owned entry count " + ownedEntryCount, mapSize > ownedEntryCount);
            }
        });
    }

    @Override
    protected Config getConfig() {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        config.addMapConfig(mapConfig);
        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = super.newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(false);

        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return nearCacheConfig;
    }

    @Override
    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setCacheLocalEntries(true);

        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(size);

        return nearCacheConfig;
    }

    /**
     * The EE Near Cache evicts a single entry per eviction.
     */
    @Override
    protected int getExpectedEvictionCount(int size) {
        return 1;
    }
}
