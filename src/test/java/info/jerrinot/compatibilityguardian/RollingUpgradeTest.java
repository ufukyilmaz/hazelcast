package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

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
}
