package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexPerfTest extends HazelcastTestSupport {

    private static final int DURATION_MILLIS = 30 * 1000;
    private static final int PERSON_COUNT = 1000000;

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Ignore
    @Test
    public void binary_sorted() {
        doTest(BINARY, true);
    }

    @Ignore
    @Test
    public void binary_unsorted() {
        doTest(BINARY, false);
    }

    @Ignore
    @Test
    public void native_sorted() {
        doTest(NATIVE, true);
    }

    @Ignore
    @Test
    public void native_unsorted() {
        doTest(NATIVE, false);
    }

    private void doTest(InMemoryFormat format, boolean sorted) {
        IMap<Integer, Person> map = getInstance(format, sorted).getMap("default");

        System.err.println("Setup started [" + format + "]");
        for (int i = 0; i < PERSON_COUNT; i++) {
            map.put(i, new Person(i));
        }
        System.err.println("Setup finished [" + format + "]");


        System.err.println("Test started [" + format + "]");
        long count = 0;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < DURATION_MILLIS) {
            map.values(Predicates.equal("age", 10));
            count++;
        }
        System.err.println("Test finished [" + format + "]");
        System.err.println("Count for " + PERSON_COUNT + " elements and format [" + format + "] = " + count);
    }

    private HazelcastInstance getInstance(InMemoryFormat format, boolean sorted) {
        Config config = getConfig();
        config.getNativeMemoryConfig().setSize(MemorySize.parse("4", MemoryUnit.GIGABYTES));
        config.getMapConfig("default").setInMemoryFormat(format);

        IndexConfig indexConfig = new IndexConfig();
        indexConfig.setType(sorted ? IndexType.SORTED : IndexType.HASH);
        indexConfig.addAttribute("age");

        config.getMapConfig("default").addIndexConfig(indexConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
//        factory.newHazelcastInstance(config);
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
