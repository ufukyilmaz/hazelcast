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

package com.hazelcast.client.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.CompatibilityTestHazelcastFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ClientCacheConfigCompatibilityTest {

    private CompatibilityTestHazelcastInstanceFactory memberFactory;
    private CompatibilityTestHazelcastFactory factory;

    @Before
    public void setup() {
        memberFactory = new CompatibilityTestHazelcastInstanceFactory(new String[] {"3.9.4", "3.10", "3.9.4"});
        factory = new CompatibilityTestHazelcastFactory();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
        memberFactory.terminateAll();
    }

    @Test
    public void testCacheConfig_deserializedProperlyOnClient_from39() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig()
                .setName("cacheBak0*")
                .setBackupCount(0)
                .setStatisticsEnabled(false)
                .setHotRestartConfig(new HotRestartConfig().setEnabled(false).setFsync(false))
                .setEvictionConfig(new EvictionConfig(2000000000, ENTRY_COUNT, LFU)));
        HazelcastInstance hz39 = memberFactory.newHazelcastInstance(config);
        HazelcastInstance hz310 = memberFactory.newHazelcastInstance(config);
        waitClusterForSafeState(hz310);

        // make 3.10 master
        hz39.shutdown();
        waitClusterForSafeState(hz310);

        // start another 3.9.x member
        hz39 = memberFactory.newHazelcastInstance(config);
        waitClusterForSafeState(hz310);
        InetSocketAddress hz39Address = (InetSocketAddress) hz39.getLocalEndpoint().getSocketAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                    .addAddress(hz39Address.getHostName() + ":" + hz39Address.getPort());
        HazelcastInstance client310 = factory.newHazelcastClient(CURRENT_VERSION, clientConfig);
        for (int i = 0; i < 10000; i++) {
            ICache cache = client310.getCacheManager().getCache("cacheBak0-" + i);
            assertNotNull(cache);
        }
    }


}
