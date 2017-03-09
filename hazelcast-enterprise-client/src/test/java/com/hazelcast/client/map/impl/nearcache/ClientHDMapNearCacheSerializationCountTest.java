package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
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
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDMapNearCacheSerializationCountTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";
    private static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger();
    private static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger();

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

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
        IMap<String, SerializationCountingData> map = client.getMap(MAP_NAME);

        String key = randomString();
        SerializationCountingData value = new SerializationCountingData();
        map.put(key, value);
        assertAndReset(1, 0);

        map.get(key);
        assertAndReset(0, 1);

        map.get(key);
        assertAndReset(0, 1);
    }

    private ClientConfig newClientConfig(String mapName) {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(true)
                .setName(mapName);

        nearCacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return new ClientConfig()
                .setNativeMemoryConfig(memoryConfig)
                .addNearCacheConfig(nearCacheConfig);
    }

    private static void prepareSerializationConfig(SerializationConfig serializationConfig) {
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

    private static void assertAndReset(int serializeCount, int deserializeCount) {
        assertEquals(serializeCount, SERIALIZE_COUNT.getAndSet(0));
        assertEquals(deserializeCount, DESERIALIZE_COUNT.getAndSet(0));
    }

    private static class SerializationCountingData implements Portable {

        private static int FACTORY_ID = 1;
        private static int CLASS_ID = 1;

        SerializationCountingData() {
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
