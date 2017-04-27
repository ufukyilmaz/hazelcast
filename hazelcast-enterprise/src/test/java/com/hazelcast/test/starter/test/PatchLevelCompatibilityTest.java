package com.hazelcast.test.starter.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class PatchLevelCompatibilityTest {

    @Test
    public void testAll_V37_Versions() {
        String[] versions = new String[]{"3.7", "3.7.1", "3.7.2", "3.7.3", "3.7.4", "3.7.5", "3.7.6", "3.7.7"};
        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.newHazelcastInstance(versions[i]);
        }
        assertClusterSizeEventually(versions.length, instances[0]);
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Test
    public void testAll_V38_Versions() {
        String[] versions = new String[]{"3.8", "3.8.1"};
        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.newHazelcastInstance(versions[i]);
        }
        assertClusterSizeEventually(versions.length, instances[0]);
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Test
    public void testMap_whenMixed_V37_Cluster() {
        HazelcastInstance hz374 = HazelcastStarter.newHazelcastInstance("3.7.4");
        HazelcastInstance hz375 = HazelcastStarter.newHazelcastInstance("3.7.5");

        IMap<Integer, String> map374 = hz374.getMap("myMap");
        map374.put(42, "GUI = Cheating!");

        IMap<Integer, String> myMap = hz375.getMap("myMap");
        String ancientWisdom = myMap.get(42);

        assertEquals("GUI = Cheating!", ancientWisdom);
        hz374.shutdown();
        hz375.shutdown();
    }
}
