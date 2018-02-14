/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDLegacyCacheSplitBrainTest extends LegacyCacheSplitBrainTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE, LatestAccessCacheMergePolicy.class},
                {NATIVE, HigherHitsCacheMergePolicy.class},
                {NATIVE, PutIfAbsentCacheMergePolicy.class},
                {NATIVE, PassThroughCacheMergePolicy.class},
                {NATIVE, CustomCacheMergePolicy.class}
        });
    }

    @Override
    protected CacheConfig newCacheConfig(String cacheName,
                                         Class<? extends CacheMergePolicy> mergePolicy, InMemoryFormat inMemoryFormat) {
        CacheConfig config = super.newCacheConfig(cacheName, mergePolicy, inMemoryFormat);
        config.getEvictionConfig().setMaximumSizePolicy(USED_NATIVE_MEMORY_SIZE);
        return config;
    }

    @Override
    protected Config config() {
        Config config = super.config();
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(MEMORY_SIZE);

        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }
}
