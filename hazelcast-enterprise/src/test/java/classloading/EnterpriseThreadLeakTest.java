package classloading;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.HDTestSupport.getHDConfig;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseThreadLeakTest extends ThreadLeakTest {

    @Test
    @Override
    public void testThreadLeak() {
        Config config = getHDConfig();

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        hz.shutdown();

        assertHazelcastThreadShutdown(oldThreads);
    }
}
