package com.hazelcast.map;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.NativeMemoryTestUtil.assertFreeNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexMemoryLeakTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 111;
    private static final int TIMEOUT_SECONDS = 30;
    private static final String MAP_NAME = "hd-map-with-index";
    private static final NativeMemoryConfig.MemoryAllocatorType ALLOCATOR_TYPE = STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(2, MemoryUnit.MEGABYTES);

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Test
    public void smoke() {
        Config config = createConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);

        IMap<Long, IndexPerson> map = hz.getMap(MAP_NAME);

        for (long id = 0; id < 100; id++) {
            map.set(id, initAndGet(id));
        }

        String[] fields = {"personId", "age", "firstName", "lastName", "salary", "count"};
        for (String field : fields) {
            map.addIndex(IndexType.HASH, field);
            map.addIndex(IndexType.SORTED, field);
        }

        map.clear();
        map.destroy();

        assertFreeNativeMemory(hz);
    }

    @Test
    public void index_addition_does_not_leak_hd_memory() throws InterruptedException {
        Config config = createConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Long, IndexPerson> map = hz1.getMap(MAP_NAME);

        AtomicLong ID = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean(false);

        Runnable addPerson = () -> {
            while (!stop.get()) {
                long id = ID.incrementAndGet();
                map.set(id, initAndGet(id));
            }
        };

        Runnable indexPersonField = () -> {
            Random random = new Random();
            while (!stop.get()) {
                String[] fields = {"personId", "age", "firstName", "lastName", "salary", "count"};
                for (String field : fields) {
                    if (random.nextBoolean()) {
                        map.addIndex(IndexType.SORTED, field);
                    } else {
                        map.addIndex(IndexType.HASH, field);
                    }
                }
            }
        };

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 4; i++) {
            executorService.submit(addPerson);
        }
        executorService.submit(indexPersonField);

        sleepSeconds(TIMEOUT_SECONDS);

        stop.set(true);

        executorService.shutdown();
        if (!executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        map.clear();
        map.destroy();

        assertFreeNativeMemory(hz1, hz2);
    }


    private static IndexPerson initAndGet(long id) {
        return new IndexPerson(id, id, "first" + id, "last" + id, id);
    }

    private Config createConfig() {
        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setStatisticsEnabled(true)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);

        mapConfig.getEvictionConfig()
                .setEvictionPolicy(LRU)
                .setSize(80)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setMetadataSpacePercentage(60)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
        return config
                .setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }
}
