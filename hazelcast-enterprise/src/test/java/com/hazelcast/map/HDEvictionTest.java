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
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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


    @Test
    public void testEvictionLFU_statisticsDisabled() {
        testEvictionLFUInternal(true);
    }

    protected void testEvictionLFUInternal(boolean disableStats) {
        String mapName = randomMapName();
        final int size = 10000;

        Config cfg = getConfig();
        cfg.setProperty(GroupProperty.PARTITION_COUNT, "1");

        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setStatisticsEnabled(disableStats);
        mc.setEvictionPolicy(EvictionPolicy.LFU);
        mc.setEvictionPercentage(20);
        mc.setMinEvictionCheckMillis(0);

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        HazelcastInstance node = createHazelcastInstance();
        final IMap<Object, Object> map = node.getMap(mapName);
        // these are frequently used entries.
        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
            map.get(i);
        }
        // expecting these entries to be evicted.
        for (int i = size / 2; i < size + 1; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse("No eviction!?!?!?", map.size() < size);
            }
        });

        // these entries should exist in map after evicting LFU.
        for (int i = 0; i < size / 2; i++) {
            assertNotNull(map.get(i));
        }
    }

    @Override
    public void testEvictionLFU() {
        testEvictionLFUInternal(false);
    }
}
