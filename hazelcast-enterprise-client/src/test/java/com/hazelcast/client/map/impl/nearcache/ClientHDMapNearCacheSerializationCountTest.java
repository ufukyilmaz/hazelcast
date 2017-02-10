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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientHDMapNearCacheSerializationCountTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = "default";
    protected static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger();
    protected static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger();

    protected TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    protected IMap<String, SerializationCountingData> map;

    @After
    public void tearDown() {
        DESERIALIZE_COUNT.set(0);
        SERIALIZE_COUNT.set(0);
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDeserializationCountWith_NativeNearCache_invalidateLocalUpdatePolicy() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = newClientConfig(MAP_NAME);
        prepareSerializationConfig(config.getSerializationConfig());

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        map = client.getMap(MAP_NAME);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        map.put(key, value);
        assertAndReset(1, 0);

        map.get(key);
        assertAndReset(0, 1);

        map.get(key);
        assertAndReset(0, 1);
    }

    protected ClientConfig newClientConfig(String mapName) {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig();
        memoryConfig.setEnabled(true);
        memoryConfig.setSize(new MemorySize(32, MemoryUnit.MEGABYTES));
        memoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(90);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setName(mapName);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setNativeMemoryConfig(memoryConfig);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        return clientConfig;
    }

    protected void prepareSerializationConfig(SerializationConfig serializationConfig) {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(SerializationCountingData.FACTORY_ID,
                ClientMapNearCacheSerializationCountTest.SerializationCountingData.CLASS_ID).build();
        serializationConfig.addClassDefinition(classDefinition);

        serializationConfig.addPortableFactory(SerializationCountingData.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new SerializationCountingData();
            }
        });
    }

    protected void assertAndReset(int serializeCount, int deserializeCount) {
        assertEquals(serializeCount, SERIALIZE_COUNT.getAndSet(0));
        assertEquals(deserializeCount, DESERIALIZE_COUNT.getAndSet(0));
    }

    protected static class SerializationCountingData implements Portable {

        static int FACTORY_ID = 1;
        static int CLASS_ID = 1;

        public SerializationCountingData() {
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            SERIALIZE_COUNT.incrementAndGet();
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            DESERIALIZE_COUNT.incrementAndGet();
        }
    }
}
