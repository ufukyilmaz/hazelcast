package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.HDTestSupport.getHDIndexConfig;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static com.hazelcast.test.Accessors.getNode;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEvictionTest extends EvictionTest {

    @Parameterized.Parameter(value = 2)
    public String globalIndex;

    @Parameterized.Parameters(name = "statisticsEnabled:{0}, perEntryStatsEnabled:{1}, globalIndex:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, true, "true"},
                {true, false, "false"},
                {false, true, "true"},
                {false, false, "false"},
        });
    }

    @Override
    boolean updateRecordAccessTime() {
        return globalIndex.equals("false");
    }

    @Override
    protected Config getConfig() {
        Config config = getHDIndexConfig(super.getConfig());
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }

    @Override
    protected MapConfig newMapConfig(String mapName) {
        return super.newMapConfig(mapName).
                setStatisticsEnabled(statisticsEnabled)
                .setPerEntryStatsEnabled(perEntryStatsEnabled)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Override
    @Ignore
    public void testLastAddedKey_canBeEvicted_whenFreeHeapNeeded() {
        // Not applicable
    }

    @Test
    @Override
    public void testEviction_increasingEntrySize() {
        int maxSizeMB = 2;
        String mapName = randomMapName();

        MapConfig mapConfig = new MapConfig(mapName + "*").setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.getEvictionConfig()
                .setComparator((o1, o2) -> 0)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                .setSize(maxSizeMB);

        Config config = getConfig().addMapConfig(mapConfig);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.MAP_EVICTION_BATCH_SIZE.getName(), "2");

        config.getNativeMemoryConfig().setSize(new MemorySize(128, MEGABYTES));
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, byte[]> map = instance.getMap(mapName);

        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
        MemoryStats memoryStats = nodeExtension.getMemoryManager().getMemoryStats();

        AtomicInteger entryEvictedEventCount = new AtomicInteger(0);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            public void entryEvicted(EntryEvent event) {
                entryEvictedEventCount.incrementAndGet();
            }
        }, false);

        int perIterationIncrement = 2048;
        List<Integer> memoryCosts = calculateEntryMemoryCosts(instance);
        long beforeUsedMemory = memoryStats.getUsedNative();
        for (int i = 0; i < 100; i++) {
            // Make sure eviction and deferred memory reclamation are
            // completed for the previous map.put(...), so memory stats
            // return the up-to-date information. Eviction and memory
            // reclamation are running on the partition thread, but they
            // do their job after map.put(...) returns. The map.get(...)
            // below serves as a barrier, it's running on the same
            // partition thread.
            map.get(i);

            int payloadSizeBytes = i * perIterationIncrement;
            long beforePutTotalUsed = memoryStats.getUsedNative() - beforeUsedMemory;
            map.put(i, new byte[payloadSizeBytes]);

            if (beforePutTotalUsed + memoryCosts.get(i) > MemoryUnit.MEGABYTES.toBytes(maxSizeMB)) {
                assertTrueEventually(() -> assertTrue(entryEvictedEventCount.get() == 2));
                entryEvictedEventCount.set(0);
            } else {
                assertTrue(entryEvictedEventCount.get() == 0);
            }
        }
    }

    private List<Integer> calculateEntryMemoryCosts(HazelcastInstance instance) {
        String mapName = randomMapName();
        // default no eviction
        MapConfig mapConfig = new MapConfig(mapName + "*").setInMemoryFormat(InMemoryFormat.NATIVE);
        Config config = getConfig().addMapConfig(mapConfig);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        IMap<Integer, byte[]> map = instance.getMap(mapName);

        List<Integer> liveSizes = new ArrayList<>();
        int perIterationIncrement = 2048;

        HiDensityStorageInfo hdStorageInfo = ((EnterpriseMapContainer) ((MapService)
                ((MapProxyImpl) map).getService()).getMapServiceContext().getMapContainer(mapName)).getHDStorageInfo();
        for (int i = 0; i < 100; i++) {
            int payloadSizeBytes = i * perIterationIncrement;
            long start = hdStorageInfo.getUsedMemory();
            map.put(i, new byte[payloadSizeBytes]);
            long end = hdStorageInfo.getUsedMemory();
            int sizeOfJustAdded = (int) (end - start);
            liveSizes.add(sizeOfJustAdded);
        }
        return liveSizes;
    }

    @Test
    public void testForceEviction_withIndexes() {
        assumeTrue(globalIndex.equals("false"));
        // never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.getEvictionConfig().setEvictionPolicy(LFU).setSize(mapMaxSize);
        mapConfig.addIndexConfig(new IndexConfig().addAttribute("age").setType(IndexType.SORTED));

        config.getNativeMemoryConfig().setAllocatorType(STANDARD);
        // 640K ought to be enough for anybody
        config.getNativeMemoryConfig()
                .setSize(new MemorySize(640, KILOBYTES));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        // now let's insert more than it can fit into a memory
        for (int i = 0; i < 2000; i++) {
            map.put(i, new Person(i));
        }

        // let's check not everything was evicted
        // this is an extra step, the main goal is to not fail with NativeOutOfMemoryError
        assertTrue(map.size() > 0);
    }

    @Test
    public void testEviction_withOrderedIndexes() {
        assumeTrue(globalIndex.equals("false"));
        testEviction_withIndexes(true);
    }

    @Test
    public void testEviction_withUnorderedIndexes() {
        assumeTrue(globalIndex.equals("false"));
        testEviction_withIndexes(false);
    }

    @Test
    public void name() {
        assumeTrue(globalIndex.equals("false"));
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), "false");
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(MAX_VALUE));

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addIndexConfig(new IndexConfig().addAttribute("age").setType(IndexType.HASH));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Person> map = node.getMap(mapName);

        map.put(5, new Person(10), 2L, SECONDS, 5L, SECONDS);

        sleepAtLeastSeconds(10);

        Collection<Person> valuesAge10 = map.values(Predicates.equal("age", 10));
        assertTrue(valuesAge10.isEmpty());
    }

    private void testEviction_withIndexes(boolean ordered) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(MAX_VALUE));

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addIndexConfig(new IndexConfig().addAttribute("age").setType(ordered ? IndexType.SORTED : IndexType.HASH));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Person> map = node.getMap(mapName);

        // the entry should be evicted based on maxIdle
        map.put(1, new Person(37), 0L, SECONDS, 1L, SECONDS);
        sleepAtLeastSeconds(2);
        Collection<Person> valuesAge37 = map.values(Predicates.equal("age", 37));
        assertTrue(valuesAge37.isEmpty());

        // the entry should be evicted based on ttl
        map.put(2, new Person(50), 1L, SECONDS);
        // the entry should stay indefinite
        map.put(3, new Person(20));
        // the entry should be evicted based on ttl
        map.put(5, new Person(10), 2L, SECONDS, 5L, SECONDS);

        // the key should stay since we replace it later
        map.put(6, new Person(5), 0L, SECONDS, 15L, SECONDS);
        // check indirect access through the HDRecord in the index
        Collection<Person> persons = map.values(Predicates.equal("age", 5));
        assertEquals(1, persons.size());
        assertEquals(5, persons.iterator().next().age);
        // check replacement without ttl/maxIdle
        map.replace(6, new Person(2));
        assertEquals(1, map.values(Predicates.equal("age", 2)).size());

        sleepAtLeastSeconds(10);

        assertTrueEventually(() -> {
            Collection<Person> valuesAge50 = map.values(Predicates.equal("age", 50));
            assertTrue(valuesAge50.isEmpty());

            Collection<Person> valuesAge10 = map.values(Predicates.equal("age", 10));
            assertTrue(valuesAge10.isEmpty());

            Collection<Person> valuesOlder15 = map.values(Predicates.greaterThan("age", 15));
            assertEquals(map.size() + " mmm", 1, valuesOlder15.size());
        }, 20);
        assertFalse(map.containsKey(1));
        assertFalse(map.containsKey(2));
        assertTrue(map.containsKey(3));
        assertTrue(map.containsKey(6));
    }

    @Override
    @Test
    public void testMaxIdle_readThroughBitmapIndex() {
        // bitmap indexes are not supported by HD
    }

    public static class Person implements DataSerializable {

        private int age;

        // hand picked size - don't change!!
        private long[] rands = new long[6];

        public Person() {
        }

        public Person(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person person = (Person) o;
            return age == person.age;
        }

        @Override
        public int hashCode() {
            return age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(age);
            out.writeLongArray(rands);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.age = in.readInt();
            this.rands = in.readLongArray();
        }
    }
}
