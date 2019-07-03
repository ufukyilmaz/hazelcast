package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.map.impl.operation.WithForcedEviction;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.max;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;


@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEvictionTest extends EvictionTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Override
    protected MapConfig newMapConfig(String mapName) {
        return super.newMapConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test
    public void testForceEviction() {
        testForcedEvictionWithRetryCount(5);
    }

    @Test
    public void testForceEviction_with_no_retry() {
        testForcedEvictionWithRetryCount(0);
    }

    @Override
    @Ignore
    public void testLastAddedKey_canBeEvicted_whenFreeHeapNeeded() {
        // Not applicable
    }

    private void testForcedEvictionWithRetryCount(int forcedEvictionRetryCount) {
        // never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(WithForcedEviction.PROP_FORCED_EVICTION_RETRY_COUNT,
                valueOf(forcedEvictionRetryCount));
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.getMaxSizeConfig().setSize(mapMaxSize);

        // 640K ought to be enough for anybody
        config.getNativeMemoryConfig().setSize(new MemorySize(640, KILOBYTES));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        // now let's insert more than it can fit into a memory
        for (int i = 0; i < 20000; i++) {
            map.put(i, i);
        }

        // let's check not everything was evicted
        // this is an extra step, the main goal is to not fail with NativeOutOfMemoryError
        assertTrue(map.size() > 0);
    }

    @Test
    @Override
    public void testEviction_increasingEntrySize() {
        int maxSizeMB = 10;
        String mapName = randomMapName();

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSizeMB, MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
        MapConfig mapConfig = new MapConfig(mapName + "*").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMaxSizeConfig(maxSizeConfig).setEvictionPolicy(EvictionPolicy.LRU);
        Config config = getConfig().addMapConfig(mapConfig);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(GroupProperty.MAP_EVICTION_BATCH_SIZE.getName(), "2");

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, byte[]> map = instance.getMap(mapName);

        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
        MemoryStats memoryStats = nodeExtension.getMemoryManager().getMemoryStats();

        int perIterationIncrement = 2048;
        long maxObservedNativeCost = 0;
        for (int i = 0; i < 1000; i++) {
            int payloadSizeBytes = i * perIterationIncrement;
            map.put(i, new byte[payloadSizeBytes]);
            maxObservedNativeCost = max(maxObservedNativeCost, memoryStats.getUsedNative());
        }

        double toleranceFactor = 1.2d;
        long maxAllowedNativeCost = (long) (MemoryUnit.MEGABYTES.toBytes(maxSizeMB) * toleranceFactor);
        long minAllowedNativeCost = (long) (MemoryUnit.MEGABYTES.toBytes(maxSizeMB) / toleranceFactor);
        assertBetween("Maximum cost", maxObservedNativeCost, minAllowedNativeCost, maxAllowedNativeCost);
    }

    @Test
    public void testForceEviction_withIndexes() {
        // never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.getMaxSizeConfig().setSize(mapMaxSize);
        mapConfig.addMapIndexConfig(new MapIndexConfig().setAttribute("age").setOrdered(true));

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
        testEviction_withIndexes(true);
    }

    @Test
    public void testEviction_withUnorderedIndexes() {
        testEviction_withIndexes(false);
    }

    private void testEviction_withIndexes(boolean ordered) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(MAX_VALUE));

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addMapIndexConfig(new MapIndexConfig().setAttribute("age").setOrdered(ordered));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Person> map = node.getMap(mapName);

        // the entry should be evicted based on maxIdle
        map.put(1, new Person(37), 0L, SECONDS, 1L, SECONDS);
        // the entry should be evicted based on ttl
        map.put(2, new Person(50), 1L, SECONDS);
        // the entry should stay indefinite
        map.put(3, new Person(20));
        // the entry will live indefinite since we touch it every 200 millisecs
        map.put(4, new Person(40), 0L, SECONDS, 2L, SECONDS);
        // the entry should be evicted based on ttl
        map.put(5, new Person(10), 2L, SECONDS, 5L, SECONDS);


        map.put(6, new Person(5), 0L, SECONDS, 2L, SECONDS);
        // check indirect access through the HDRecord in the index
        Collection<Person> persons = map.values(Predicates.equal("age", 5));
        assertEquals(1, persons.size());
        assertEquals(5, persons.iterator().next().age);
        // check replacement without ttl/maxIdle
        map.replace(6, new Person(2));
        assertEquals(1, map.values(Predicates.equal("age", 2)).size());

        assertTrueEventually(() -> {
            Collection<Person> valuesAge37 = map.values(Predicates.equal("age", 37));
            assertTrue(valuesAge37.isEmpty());

            Collection<Person> valuesAge50 = map.values(Predicates.equal("age", 50));
            assertTrue(valuesAge50.isEmpty());

            Collection<Person> valuesAge40 = map.values(Predicates.equal("age", 40));
            assertFalse(valuesAge40.isEmpty());

            Collection<Person> valuesAge10 = map.values(Predicates.equal("age", 10));
            assertFalse(valuesAge10.isEmpty());

            Collection<Person> valuesOlder15 = map.values(Predicates.greaterThan("age", 15));
            assertEquals(2, valuesOlder15.size());
        });
        assertFalse(map.containsKey(1));
        assertFalse(map.containsKey(2));
        assertTrue(map.containsKey(3));
        assertTrue(map.containsKey(6));
    }

    @Ignore
    @Test
    public void testMaxIdle_readThroughOrderedIndex() {
        // no-op
    }

    @Ignore
    @Test
    public void testMaxIdle_readThroughUnorderedIndex() {
        // no-op
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
