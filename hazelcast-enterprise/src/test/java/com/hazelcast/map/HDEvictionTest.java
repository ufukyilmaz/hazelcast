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
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDEvictionTest extends EvictionTest {

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }


    @Override
    public void testEvictionLFU() {
        testEvictionLFUInternal(false);
    }

    @Test
    public void testEvictionLFU_statisticsDisabled() {
        testEvictionLFUInternal(true);
    }

    /**
     * This test is only testing occurrence of LFU eviction.
     */
    protected void testEvictionLFUInternal(boolean disableStats) {
        int mapMaxSize = 10000;
        String mapName = randomMapName();

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(PER_NODE);
        msc.setSize(mapMaxSize);

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setStatisticsEnabled(disableStats);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.setMinEvictionCheckMillis(0);
        mapConfig.setMaxSizeConfig(msc);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        for (int i = 0; i < 2 * mapMaxSize; i++) {
            map.put(i, i);
        }

        int mapSize = map.size();
        assertTrue("Eviction did not work, map size " + mapSize + " should be smaller than allowed max size = " + mapMaxSize,
                mapSize < mapMaxSize);
    }
}
