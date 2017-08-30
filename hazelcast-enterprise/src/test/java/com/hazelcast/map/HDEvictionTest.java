package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDEvictionTest extends EvictionTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Test
    public void testForceEviction() {
        //never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = Integer.MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.getMaxSizeConfig().setSize(mapMaxSize);

        //640K ought to be enough for anybody
        config.getNativeMemoryConfig().setSize(new MemorySize(640, KILOBYTES));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        //now let's insert more than it can fit into a memory
        for (int i = 0; i < 20000; i++) {
            map.put(i, i);
        }

        //let's check not everything was evicted.
        //this is an extra step. the main goal is to not fail with NativeOutOfMemoryError
        assertTrue(map.size() > 0);
    }

    @Test
    public void testForceEviction_withIndexes() {
        //never run an explicit eviction -> rely on forced eviction instead
        int mapMaxSize = Integer.MAX_VALUE;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "101");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.getMaxSizeConfig().setSize(mapMaxSize);
        mapConfig.addMapIndexConfig(new MapIndexConfig().setAttribute("age").setOrdered(true));

        //640K ought to be enough for anybody
        config.getNativeMemoryConfig()
              .setSize(new MemorySize(640, KILOBYTES));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        //now let's insert more than it can fit into a memory
        for (int i = 0; i < 2000; i++) {
            map.put(i, new Person(i));
        }

        //let's check not everything was evicted.
        //this is an extra step. the main goal is to not fail with NativeOutOfMemoryError
        assertTrue(map.size() > 0);
    }

    public static class Person implements DataSerializable {
        private int age;
        // Hand picked size - don't change!!
        public long[] rands = new long[6];

        public Person() {
        }

        public Person(int age) {
            this.age = age;
        }

        public int getAge() {
            return age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HDEvictionTest.Person person = (HDEvictionTest.Person) o;
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
