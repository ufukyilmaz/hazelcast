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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.spi.properties.GroupProperty.ENTERPRISE_LICENSE_KEY;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertTrue;

/**
 * Uses imap and near cache internals to stress near cache native memory.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class ClientMapHDNearCacheStressTest extends HazelcastTestSupport {

    private static final String MAP_NAME = ClientMapHDNearCacheStressTest.class.getName();
    private static final long NEAR_CACHE_NATIVE_MEMORY_MEGABYTES = 64;
    private static final int NEAR_CACHE_PUT_COUNT = 5000000;
    private static final long TEST_TIMEOUT_TEN_MINUTES = 10 * 60 * 1000;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private IMap clientMap;

    @Before
    public void setUp() throws Exception {
        factory.newHazelcastInstance();

        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(newNearCacheConfig());

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        clientMap = client.getMap(MAP_NAME);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test(timeout = TEST_TIMEOUT_TEN_MINUTES)
    public void near_cache_does_not_throw_native_oome() throws Exception {
        NearCachedClientMapProxy proxy = (NearCachedClientMapProxy) this.clientMap;
        SerializationService ss = proxy.getClientContext().getSerializationService();
        NearCache nearCache = proxy.getNearCache();

        for (int i = 0; i < NEAR_CACHE_PUT_COUNT; i++) {
            Data key = ss.toData(i);
            long reservationId = nearCache.tryReserveForUpdate(key);
            if (reservationId != NOT_RESERVED) {
                try {
                    nearCache.tryPublishReserved(key, ss.toData(i), reservationId, true);
                } catch (Throwable throwable) {
                    nearCache.remove(key);
                    throw rethrow(throwable);
                }
            }
        }

        assertTrue(nearCache.size() > 0);
    }

    private static NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME);
        nearCacheConfig.setInMemoryFormat(NATIVE);
        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaximumSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return nearCacheConfig;
    }

    private static ClientConfig newClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(NEAR_CACHE_NATIVE_MEMORY_MEGABYTES, MEGABYTES))
                .setAllocatorType(POOLED).setMetadataSpacePercentage(40);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        clientConfig.setNativeMemoryConfig(nativeMemoryConfig);

        return clientConfig;
    }

}
