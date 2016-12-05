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
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
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
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDNearCacheTest extends NearCacheTest {

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

    @Test
    @Override
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        // this test expects an IllegalArgumentException in OS, but should not throw any exception in EE
        super.testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException();
    }
}
