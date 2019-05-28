package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexLeakTest
        extends HazelcastTestSupport {

    private static final int PERSON_COUNT = 1000;

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Test
    public void native_sorted() {
        doTest(NATIVE, true);
    }

    @Test
    public void native_unsorted() {
        doTest(NATIVE, false);
    }

    private void doTest(InMemoryFormat format, boolean sorted) {
        HazelcastInstance instance = getInstance(format, sorted);
        MemoryStats stats = ((HazelcastInstanceProxy) instance).getOriginal().getMemoryStats();

        assertEquals(0, stats.getUsedNative());

        IMap<Integer, Person> map = instance.getMap("default");
        for (int i = 0; i < PERSON_COUNT; i++) {
            map.put(i, new Person(i));
        }

        assertNotEquals(0, stats.getUsedNative());

        map.destroy();

        assertEquals(0, stats.getUsedNative());
    }

    private HazelcastInstance getInstance(InMemoryFormat format, boolean sorted) {
        Config config = getConfig();
        config.getNativeMemoryConfig().setSize(MemorySize.parse("4", MemoryUnit.GIGABYTES));
        config.getMapConfig("default").setInMemoryFormat(format);

        MapIndexConfig indexConfig = new MapIndexConfig();
        indexConfig.setOrdered(sorted);
        indexConfig.setAttribute("age");

        config.getMapConfig("default").addMapIndexConfig(indexConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        return factory.newHazelcastInstance(config);
    }

    public static class Person implements DataSerializable {
        private int age;

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
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.age = in.readInt();
        }
    }
}
