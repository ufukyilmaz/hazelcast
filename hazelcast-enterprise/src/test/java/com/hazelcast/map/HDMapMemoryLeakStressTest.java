package com.hazelcast.map;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Bits;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.NativeMemoryTestUtil.assertFreeNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsNotZero;
import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsZero;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static org.junit.Assert.assertEquals;

/**
 * This test is an adapted version of {@link com.hazelcast.cache.CacheNativeMemoryLeakStressTest}.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDMapMemoryLeakStressTest extends HazelcastTestSupport {

    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);
    private static final int[] OP_SET = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
    private static final int KEY_RANGE = 10000000;
    private static final int PARTITION_COUNT = 271;
    private static final int REPS = 1000;
    private static final String MAP_NAME = randomMapName("HD");

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Test
    @Category(QuickTest.class)
    public void test_shutdown() {
        final Config config = createConfig();
        config.getNativeMemoryConfig().setAllocatorType(MemoryAllocatorType.POOLED);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);

        final IMap<Object, Object> map = hz.getMap(MAP_NAME);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        MemoryStats memoryStats = getNode(hz).hazelcastInstance.getMemoryStats();
        assertMemoryStatsNotZero("member", memoryStats);

        hz.shutdown();
        assertMemoryStatsZero("member", memoryStats);
    }

    @Test
    public void test_MapOperations() {
        final Config config = createConfig();

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Integer, byte[]> map = hz1.getMap(MAP_NAME);
        map.addIndex("__key", true);

        final AtomicBoolean stopBouncingThread = new AtomicBoolean(false);
        Thread bouncingThread = new Thread() {
            @Override
            public void run() {
                while (!stopBouncingThread.get()) {
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    sleepSeconds(10);
                    factory.terminate(hz);
                    sleepSeconds(5);
                }
            }
        };
        bouncingThread.start();

        int threads = 8;
        final AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch workerDoneLatch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new WorkerThread(map, workerDoneLatch, running).start();
        }

        assertOpenEventually("WorkerThreads didn't finish in time", workerDoneLatch, TIMEOUT * 2);

        stopBouncingThread.set(true);
        assertJoinable(bouncingThread);

        map.clear();
        map.destroy();

        assertFreeNativeMemory(hz1, hz2);
    }

    protected Config createConfig() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setStatisticsEnabled(true)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setMaxSizeConfig(maxSizeConfig);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        return new Config()
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    private static class WorkerThread extends Thread {

        private final IMap<Integer, byte[]> map;
        private final CountDownLatch latch;
        private final Random rand = new Random();
        private final AtomicBoolean running;

        WorkerThread(IMap<Integer, byte[]> map, CountDownLatch latch, AtomicBoolean running) {
            this.map = map;
            this.latch = latch;
            this.running = running;
        }

        @Override
        public void run() {
            int repetitions = 0;
            while (running.get() && repetitions++ <= REPS) {
                try {
                    int key = rand.nextInt(KEY_RANGE);
                    int op = rand.nextInt(OP_SET.length);
                    doOp(OP_SET[op], key);
                } catch (Throwable e) {
                    e.printStackTrace();
                    running.set(false);
                }
            }

            latch.countDown();
        }

        private void doOp(int op, int key) {
            switch (op) {
                case 0:
                    map.put(key, newValue(key));
                    break;

                case 1:
                    map.remove(key);
                    break;

                case 2:
                    map.replace(key, newValue(key));
                    break;

                case 4:
                    map.putIfAbsent(key, newValue(key));
                    break;

                case 5:
                    byte[] value = map.put(key, newValue(key));
                    verifyValue(key, value);
                    break;

                case 6:
                    value = map.remove(key);
                    verifyValue(key, value);
                    break;

                case 7:
                    value = map.replace(key, newValue(key));
                    verifyValue(key, value);
                    break;

                case 8:
                    byte[] current = map.get(key);
                    verifyValue(key, current);
                    if (current != null) {
                        map.replace(key, current, newValue(key));
                    } else {
                        map.replace(key, newValue(key));
                    }
                    break;

                case 9:
                    current = map.get(key);
                    verifyValue(key, current);
                    if (current != null) {
                        map.remove(key, current);
                    } else {
                        map.remove(key);
                    }
                    break;

                case 10:
                    Map<Integer, byte[]> entries = new HashMap<Integer, byte[]>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        entries.put(k, newValue(k));
                    }
                    map.putAll(entries);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        byte[] v = map.get(k);
                        verifyValue(k, v);
                    }
                    break;

                case 11:
                    Set<Integer> keysToAdd = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToAdd.add(k);
                    }
                    Map<Integer, byte[]> results = map.getAll(keysToAdd);
                    for (Map.Entry<Integer, byte[]> result : results.entrySet()) {
                        Integer k = result.getKey();
                        byte[] v = result.getValue();
                        verifyValue(k, v);
                    }
                    break;

                case 12:
                    map.delete(key);
                    break;

                case 13:
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        map.lock(key, 5, TimeUnit.SECONDS);
                    }
                    break;

                case 14:
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        map.put(key, newValue(key), 1, TimeUnit.SECONDS);
                    }
                    break;

                case 15:
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        map.set(key, newValue(key), 1, TimeUnit.SECONDS);
                    }
                    break;

                case 16:
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        final byte[] newValue = newValue(k);
                        Object newKey = map.executeOnKey(k, new AdderEntryProcessor(newValue));

                        verifyValue(((Integer) newKey), newValue);
                    }
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        byte[] v = map.get(k);
                        verifyValue(k, v);
                    }
                    break;

                case 17:
                    Set<Integer> keysToRemove = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToRemove.add(k);
                    }
                    map.executeOnKeys(keysToRemove, new RemoverEntryProcessor());
                    break;

                case 18:
                    EntryObject entryObject = new PredicateBuilder().getEntryObject();
                    PredicateBuilder predicate = entryObject.key().between(key, key + 32);
                    map.values(predicate);
                    break;

                default:
                    byte[] bytes = newValue(key);
                    map.put(key, bytes);
            }
        }

        private void verifyValue(int key, byte[] value) {
            if (value != null) {
                assertEquals(key, Bits.readIntB(value, 0));
                assertEquals(key, Bits.readIntB(value, value.length - 4));
            }
        }

        private byte[] newValue(int k) {
            int len = 16 + rand.nextInt(1 << 12); // up to 4k
            byte[] value = new byte[len];
            rand.nextBytes(value);

            Bits.writeIntB(value, 0, k);
            Bits.writeIntB(value, len - 4, k);

            return value;
        }
    }

    private static class AdderEntryProcessor extends AbstractEntryProcessor<Integer, byte[]> {

        private final byte[] value;

        AdderEntryProcessor(byte[] value) {
            super(false);
            this.value = value;
        }

        @Override
        public Object process(Map.Entry<Integer, byte[]> entry) {
            entry.setValue(value);
            return entry.getKey();
        }
    }

    private static class RemoverEntryProcessor extends AbstractEntryProcessor<Integer, byte[]> {

        @Override
        public Object process(Map.Entry<Integer, byte[]> entry) {
            entry.setValue(null);
            return Boolean.TRUE;
        }
    }
}