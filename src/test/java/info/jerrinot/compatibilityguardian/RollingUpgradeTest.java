package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RollingUpgradeTest {

    @Test
    public void testAllVersions() {
        String[] versions = new String[]{"3.7", "3.7.1", "3.7.2", "3.7.3", "3.7.4", "3.7.5"};
        HazelcastInstance hz = null;
        for (String version : versions) {
            hz = HazelcastStarter.startHazelcastVersion(version);
        }
        HazelcastTestSupport.assertClusterSizeEventually(6, hz);
    }

    @Test
    public void testPutAndGet() {
        HazelcastInstance hz374 = HazelcastStarter.startHazelcastVersion("3.7.4");
        HazelcastInstance hz375 = HazelcastStarter.startHazelcastVersion("3.7.5");

        IMap<Integer, String> map374 = hz374.getMap("myMap");
        map374.put(42, "UI = Cheating!");

        IMap<Integer, String> myMap = hz375.getMap("myMap");
        String ancientWisdom = myMap.get(42);

        assertEquals("UI = Cheating!", ancientWisdom);
    }
}
