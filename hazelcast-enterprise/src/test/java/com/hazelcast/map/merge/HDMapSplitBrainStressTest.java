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

package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapSplitBrainStressTest extends MapSplitBrainStressTest {

    @Override
    protected Config config() {
        Config config = getHDConfig(super.config());
        config.getNativeMemoryConfig().setSize(new MemorySize(1, MemoryUnit.GIGABYTES));

        MapConfig mapConfig = config.getMapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        return config;
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HDMapSplitBrainStressTest.super.onAfterSplitBrainHealed(instances);
            }
        }, 30);
    }
}
