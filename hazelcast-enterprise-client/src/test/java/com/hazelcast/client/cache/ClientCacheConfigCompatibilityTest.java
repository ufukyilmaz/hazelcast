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

import com.hazelcast.cache.CacheUtil;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.CompatibilityTestHazelcastFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.net.InetSocketAddress;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static org.junit.Assert.assertNotNull;

// RU_COMPAT_3_9
// Tests that a CacheConfig retrieved from a 3.9 member in a mixed 3.9-3.10 cluster
// does not cause serialization errors on a 3.10 client (usually manifesting as heap OOME due
// to allocation of a char[] buffer with huge length)
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ClientCacheConfigCompatibilityTest {

    private CompatibilityTestHazelcastInstanceFactory memberFactory;
    private CompatibilityTestHazelcastFactory factory;

    @Before
    public void setup() {
        memberFactory = new CompatibilityTestHazelcastInstanceFactory(new String[] {"3.9.4", CURRENT_VERSION});
        factory = new CompatibilityTestHazelcastFactory();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
        memberFactory.terminateAll();
    }

    @Test
    public void testCacheConfig_deserializedProperlyOnClient_from39() {
        HazelcastInstance hz39 = memberFactory.newHazelcastInstance();
        HazelcastInstance hz310 = memberFactory.newHazelcastInstance();
        waitClusterForSafeState(hz310);

        InetSocketAddress hz39Address = (InetSocketAddress) hz39.getLocalEndpoint().getSocketAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                    .setSmartRouting(false)
                    .addAddress(hz39Address.getHostName() + ":" + hz39Address.getPort());
        HazelcastInstance client310 = factory.newHazelcastClient(CURRENT_VERSION, clientConfig);

        CachingProvider member310Provider = createCachingProvider(hz310);
        CacheManager cacheManager = member310Provider.getCacheManager(null,
                HazelcastServerCachingProvider.class.getClassLoader(), propertiesByInstanceItself(hz310));

        // ensure the distributed object name of the cache is owned by 3.9 member
        // so the 3.10 client will obtain the CacheConfig from the 3.9 member
        String cacheName = randomName();
        String distributedObjectName = CacheUtil.getDistributedObjectName(cacheName);
        PartitionService hz310PartitionService = hz310.getPartitionService();
        while (hz310PartitionService.getPartition(distributedObjectName).getOwner().localMember()) {
            cacheName = randomName();
            distributedObjectName = CacheUtil.getDistributedObjectName(cacheName);
        }

        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setBackupCount(0)
                   .setEvictionConfig(new EvictionConfig(2000000000, ENTRY_COUNT, LFU))
                   .setHotRestartConfig(new HotRestartConfig().setEnabled(false).setFsync(false))
                   .setStatisticsEnabled(false);

        // create cache on 3.10 member -> propagates the CacheConfig to 3.9 member
        cacheManager.createCache(cacheName, cacheConfig);
        // obtain client-side cache proxy; this retrieves the CacheConfig from the
        // 3.9 member and deserializes it on the client
        ICache cache = client310.getCacheManager().getCache(cacheName);
        assertNotNull(cache);
    }


}
