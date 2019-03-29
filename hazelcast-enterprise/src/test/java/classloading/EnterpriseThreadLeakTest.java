package classloading;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static classloading.ThreadLeakTestUtils.assertHazelcastThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;
import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseThreadLeakTest extends ThreadLeakTest {

    @Test
    public void testThreadLeak() {
        Config config = getHDConfig();

        Set<Thread> oldThreads = getThreads();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        hz.shutdown();
        assertHazelcastThreadShutdown(oldThreads);
    }
}
