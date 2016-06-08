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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDEvictionTest extends EvictionTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }


    @Test
    public void testForceEviction() {
        //never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = Integer.MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.getMaxSizeConfig().setSize(mapMaxSize);

        //640K ought to be enough for anybody
        config.getNativeMemoryConfig().setSize(new MemorySize(640, KILOBYTES));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        //now let's insert more than it can fit into a memory
        for (int i = 0; i < 20000; i++) {
            map.put(i, i);
        }

        //let's check not everything was evicted.
        //this is an extra step. the main goal is to not fail with NativeOutOfMemoryError
        assertTrue(map.size()> 0);
    }

}
