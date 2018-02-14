/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.AssertTask;
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
public class HDCacheSplitBrainTest extends CacheSplitBrainTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameterized.Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE, DiscardMergePolicy.class},
                {NATIVE, HigherHitsMergePolicy.class},
                {NATIVE, LatestAccessMergePolicy.class},
                {NATIVE, PassThroughMergePolicy.class},
                {NATIVE, PutIfAbsentMergePolicy.class},

                {NATIVE, MergeIntegerValuesMergePolicy.class},
                {NATIVE, MergeIntegerValuesMergePolicy.class},
        });
    }

    @Override
    protected Config config() {
        Config config = super.config();

        config.getCacheConfig(cacheNameA)
                .getEvictionConfig().setMaximumSizePolicy(USED_NATIVE_MEMORY_SIZE);
        config.getCacheConfig(cacheNameB)
                .getEvictionConfig().setMaximumSizePolicy(USED_NATIVE_MEMORY_SIZE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(MEMORY_SIZE);

        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {

        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() {
                HDCacheSplitBrainTest.super.onAfterSplitBrainHealed(instances);
            }
        };

        assertTrueEventually(assertTask, 30);
    }
}
