package com.hazelcast.elasticmemory;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.StorageFormat;
import com.hazelcast.core.*;
import com.hazelcast.elasticmemory.error.OffHeapOutOfMemoryError;
import com.hazelcast.elasticmemory.util.MemorySize;
import com.hazelcast.elasticmemory.util.MemoryUnit;
import com.hazelcast.enterprise.EnterpriseJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.storage.Storage;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(EnterpriseJUnitClassRunner.class)
public class OffHeapStorageTest {

    public static final int SIZE = 1024;

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

    private void testPutGetRemove(int segmentSize, int chunkSize, boolean useUnsafe) {
        final Storage<DataRefImpl> s = useUnsafe ? new UnsafeStorage(segmentSize, chunkSize) : new ByteBufferStorage(segmentSize, chunkSize);
        final Random rand = new Random();
        final int k = 3072;

        byte[] data = new byte[k];
        rand.nextBytes(data);
        final int hash = rand.nextInt();

        final DataRefImpl ref = s.put(hash, new Data(SerializationConstants.CONSTANT_TYPE_DATA, data));
        assertEquals(k, ref.size());
        assertEquals((int) Math.ceil((double) k / (chunkSize * SIZE)), ref.getChunkCount());

        Data resultData = s.get(hash, ref);
        assertNotNull(resultData);
        byte[] result = resultData.getBuffer();
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

    @Test(expected = OffHeapOutOfMemoryError.class)
    public void testBufferOverFlow() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count + 1, false);
    }

    @Test(expected = OffHeapOutOfMemoryError.class)
    public void testBufferOverFlow2() {
        final int count = (int) (total.kiloBytes() / chunk.kiloBytes());
        fillUpBuffer(count + 1, true);
    }

    private void fillUpBuffer(int count, boolean useUnsafe) {
        final Storage s = useUnsafe ? new UnsafeStorage((int) total.megaBytes(), (int) chunk.kiloBytes())
                : new ByteBufferStorage((int) total.megaBytes(), (int) chunk.kiloBytes());
        byte[] data = new byte[(int) chunk.bytes()];
        for (int i = 0; i < count; i++) {
            s.put(i, new Data(SerializationConstants.CONSTANT_TYPE_DATA, data));
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
        c.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < SIZE; i++) {
            map.put(i, value);
        }

        map.clear();
        for (int i = 0; i < SIZE; i++) {
            map.put(i, value);
        }
        assertEquals(SIZE, map.size());
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testMapStorageOom() {
        testMapStorageOom(false);
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testMapStorageOom2() {
        testMapStorageOom(true);
    }

    private void testMapStorageOom(boolean useUnsafe) {
        Config c = new Config();
        c.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < SIZE; i++) {
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
        c.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        final byte[] value = new byte[1000];

        IMap map = hz.getMap("test");
        for (int i = 0; i < SIZE; i++) {
            map.put(i, value);
        }
        map.destroy();

        IMap map2 = hz.getMap("test2");
        for (int i = 0; i < SIZE; i++) {
            map2.put(i, value);
        }
        assertEquals(SIZE, map2.size());
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
        c.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "1M");
        c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        if (useUnsafe) {
            c.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "true");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        IMap map = hz.getMap("test");
        final byte[] value = new byte[1000];
        for (int i = 0; i < SIZE; i++) {
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
        assertEquals(SIZE, map.size());
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
        mapConfig.setStorageFormat(StorageFormat.OFFHEAP);
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
        for (int i = 0; i < SIZE; i++) {
            map.put(i, value);
        }

        latch.await(30, TimeUnit.SECONDS);

        for (int i = 0; i < extra; i++) {
            map.put(i, value);
        }
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testSharedMapStorageOom() {
        Config c1 = new Config();
        c1.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c1.getGroupConfig().setName("dev1");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        Config c2 = new Config();
        c2.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
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
            for (int i = 0; i < SIZE; i++) {
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
        c1.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
        c1.getGroupConfig().setName("dev1");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        c1.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_SHARED_STORAGE, "true");

        Config c2 = new Config();
        c2.getMapConfig("default").setStorageFormat(StorageFormat.OFFHEAP);
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
            for (int i = 0; i < SIZE; i++) {
                map.put(i, value);
            }
            hz.getLifecycleService().shutdown();

            IMap map2 = hz2.getMap("test");
            for (int i = 0; i < SIZE; i++) {
                map2.put(i, value);
            }

        } finally {
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE);
            System.clearProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE);
        }
    }
}
