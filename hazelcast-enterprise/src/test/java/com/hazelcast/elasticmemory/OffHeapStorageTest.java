package com.hazelcast.elasticmemory;

import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.internal.storage.Storage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class OffHeapStorageTest {

    public static final int ENTRY_COUNT = 1024;

    @BeforeClass
    @AfterClass
    public static void cleanupClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPutGetRemove1() {
        testPutGetRemove(1, 1, false);
    }

    @Test
    public void testPutGetRemove2() {
        testPutGetRemove(2, 2, false);
    }

    @Test
    public void testPutGetRemove4() {
        testPutGetRemove(4, 4, true);
    }

    @Test
    public void testPutGetRemove5() {
        testPutGetRemove(5, 5, true);
    }

    @Test
    public void testPutGetRemove8() {
        testPutGetRemove(2, 8, true);
    }

    @Test
    public void testPutGetRemove10() {
        testPutGetRemove(3, 10, false);
    }

    @Test
    public void testPutGetRemove15() {
        testPutGetRemove(3, 15, true);
    }

    @Test
    public void testPutGetRemove16() {
        testPutGetRemove(2, 16, false);
    }

    private void testPutGetRemove(long segmentSizeInMb, int chunkSizeInKb, boolean useUnsafe) {
        long segmentSize = MemoryUnit.MEGABYTES.toBytes(segmentSizeInMb);
        int chunkSize = (int) MemoryUnit.KILOBYTES.toBytes(chunkSizeInKb);

        final Storage<DataRefImpl> s = useUnsafe ? new UnsafeStorage(segmentSize, chunkSize) : new ByteBufferStorage(segmentSize, chunkSize);
        final Random rand = new Random();
        final int k = 3072;

        byte[] data = new byte[k];
        rand.nextBytes(data);
        final int hash = rand.nextInt();

        final DataRefImpl ref = s.put(hash, new DefaultData(data));
        assertEquals(k, ref.size());
        assertEquals((int) Math.ceil((double) k / chunkSize), ref.getChunkCount());

        Data resultData = s.get(hash, ref);
        assertNotNull(resultData);
        byte[] result = resultData.toByteArray();
        assertArrayEquals(data, result);

        s.remove(hash, ref);
        assertNull(s.get(hash, ref));

        s.destroy();
    }

    final MemorySize total = new MemorySize(32, MemoryUnit.MEGABYTES);
    final MemorySize chunk = new MemorySize(1, MemoryUnit.KILOBYTES);

    @Test
    public void testFillUpBuffer() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count, false);
    }

    @Test
    public void testFillUpBuffer2() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count, true);
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testBufferOverFlow() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count + 1, false);
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testBufferOverFlow2() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count + 1, true);
    }

    private void fillUpBuffer(int count, boolean useUnsafe) {
        final Storage s = useUnsafe ? new UnsafeStorage(total.bytes(), (int) chunk.bytes())
                : new ByteBufferStorage(total.bytes(), (int) chunk.bytes());
        byte[] data = new byte[(int) chunk.bytes()];
        for (int i = 0; i < count; i++) {
            s.put(i, new DefaultData(data));
        }
        s.destroy();
    }

    @Test
    public void testMapStorageFull() {
        testMapStorageFull(false);
    }

    @Test
    public void testMapStorageFull2() {
        testMapStorageFull(true);
    }

    private void testMapStorageFull(boolean useUnsafe) {
        Config c = new Config();
        c.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }

        map.clear();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }
        assertEquals(ENTRY_COUNT, map.size());
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testMapStorageOom() {
        testMapStorageOom(false);
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testMapStorageOom2() {
        testMapStorageOom(true);
    }

    private void testMapStorageOom(boolean useUnsafe) {
        Config c = new Config();
        c.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }
        map.put(-1, value);
    }

    @Test
    public void testMapStorageAfterDestroy() {
        testMapStorageAfterDestroy(false);
    }

    @Test
    public void testMapStorageAfterDestroy2() {
        testMapStorageAfterDestroy(true);
    }

    private void testMapStorageAfterDestroy(boolean useUnsafe) {
        Config c = new Config();
        c.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        final byte[] value = new byte[1000];

        IMap map = hz.getMap("test");
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }
        map.destroy();

        IMap map2 = hz.getMap("test2");
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map2.put(i, value);
        }
        assertEquals(ENTRY_COUNT, map2.size());
    }

    @Test
    public void testMapStorageAfterRemove() {
        testMapStorageAfterRemove(false);
    }

    @Test
    public void testMapStorageAfterRemove2() {
        testMapStorageAfterRemove(true);
    }

    private void testMapStorageAfterRemove(boolean useUnsafe) {
        Config c = new Config();
        c.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }

        for (int i = 0; i < 100; i++) {
            int k = (int) (Math.random() * 100);
            if (k < 35) {
                map.remove(i);
            } else if (k < 70) {
                map.delete(i);
            } else {
                map.evict(i);
            }
        }

        for (int i = 0; i < 100; i++) {
            map.put(i, value);
        }
        assertEquals(ENTRY_COUNT, map.size());
    }

    @Test
    public void testMapStorageAfterTTL() throws InterruptedException {
        testMapStorageAfterTTL(false);
    }

    @Test
    public void testMapStorageAfterTTL2() throws InterruptedException {
        testMapStorageAfterTTL(true);
    }

    private void testMapStorageAfterTTL(boolean useUnsafe) throws InterruptedException {
        Config c = new Config();
        MapConfig mapConfig = c.getMapConfig("default");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.setTimeToLiveSeconds(1);

        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        int extra = 100;
        final CountDownLatch latch = new CountDownLatch(extra);
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, true);

        final byte[] value = new byte[1000];
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, value);
        }

        latch.await(30, TimeUnit.SECONDS);

        for (int i = 0; i < extra; i++) {
            map.put(i, value);
        }
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testSharedMapStorageOom() {
        Config c1 = new Config();
        c1.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c1.getGroupConfig().setName("dev1");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        Config c2 = new Config();
        c2.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c2.getGroupConfig().setName("dev2");
        c2.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c2.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        try {
            System.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
            System.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");

            HazelcastInstance hz = Hazelcast.newHazelcastInstance(c1);
            HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(c2);
            final byte[] value = new byte[1000];

            IMap map = hz.getMap("test");
            for (int i = 0; i < ENTRY_COUNT; i++) {
                map.put(i, value);
            }

            hz2.getMap("test").put(1, value);
        } finally {
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE);
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE);
        }
    }

    @Test
    public void testSharedMapStorageAfterShutdown() {
        Config c1 = new Config();
        c1.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c1.getGroupConfig().setName("dev1");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        Config c2 = new Config();
        c2.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        c2.getGroupConfig().setName("dev2");
        c2.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c2.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        try {
            System.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
            System.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");

            HazelcastInstance hz = Hazelcast.newHazelcastInstance(c1);
            HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(c2);
            final byte[] value = new byte[1000];

            IMap map = hz.getMap("test");
            for (int i = 0; i < ENTRY_COUNT; i++) {
                map.put(i, value);
            }
            hz.getLifecycleService().shutdown();

            IMap map2 = hz2.getMap("test");
            for (int i = 0; i < ENTRY_COUNT; i++) {
                map2.put(i, value);
            }

        } finally {
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE);
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE);
        }
    }


    @Test
    public void testEmptyData1() {
        testEmptyData(false);
    }

    @Test
    public void testEmptyData2() {
        testEmptyData(true);
    }

    private void testEmptyData(boolean useUnsafe) {
        final Config config = new Config();
        config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }

        config.getSerializationConfig().setGlobalSerializerConfig(new GlobalSerializerConfig()
                .setImplementation(new StreamSerializer() {
                    public void write(ObjectDataOutput out, Object object) throws IOException {
                    }

                    public Object read(ObjectDataInput in) throws IOException {
                        return new SingletonValue();
                    }
                    public int getTypeId() {
                        return 123;
                    }
                    public void destroy() {
                    }
                }));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        IMap<Object, Object> map = hz.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new SingletonValue());
        }
        assertEquals(10, map.size());

        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> map2 = hz2.getMap("test");
        assertEquals(10, map2.size());
        assertEquals(10, map.size());

        for (int i = 0; i < 10; i++) {
            Object o = map2.get(i);
            assertNotNull(o);
            assertEquals(new SingletonValue(), o);
        }
    }

    private static class SingletonValue {
        public boolean equals(Object obj) {
            return obj instanceof SingletonValue;
        }
    }
}
