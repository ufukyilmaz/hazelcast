package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.TCP_JOIN_PORT_TRY_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.WAIT_SECONDS_BEFORE_JOIN;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ClusterSafeCompatibilityTest extends HazelcastTestSupport {

    private CompatibilityTestHazelcastInstanceFactory factory;
    private final String[] mapNames = {"map1", "map2", "map3", "map4", "map5"};

    @Before
    public void init() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void isClusterSafe_withoutPartitionInitialization() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertTrue(hz.getPartitionService().isClusterSafe());
        }
    }

    @Test
    public void isClusterSafe_withPartitionInitialization() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        warmUpPartitions(instances);

        for (HazelcastInstance hz : instances) {
            assertTrue(hz.getPartitionService().isClusterSafe());
        }
    }

    @Test
    public void isClusterSafe_withEmptyPartitionMigrations() {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        warmUpPartitions(instance);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertClusterSafeEventually(hz);
        }
        assertClusterSafeEventually(instance);
    }

    @Test
    public void isClusterSafe_withData() {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        fillMaps(instance);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertClusterSafeEventually(hz);
        }
        assertClusterSafeEventually(instance);
    }

    private void assertClusterSafeEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(instance.getPartitionService().isClusterSafe());
            }
        });
    }

    @Test
    public void isMemberSafe_withoutPartitionInitialization() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertTrue(hz.getPartitionService().isLocalMemberSafe());
        }
    }

    @Test
    public void isMemberSafe_withPartitionInitialization() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        warmUpPartitions(instances);

        for (HazelcastInstance hz : instances) {
            assertTrue(hz.getPartitionService().isLocalMemberSafe());
        }
    }

    @Test
    public void isMemberSafe_withEmptyPartitionMigrations() {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        warmUpPartitions(instance);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertLocalMemberSafeEventually(hz);
        }
        assertLocalMemberSafeEventually(instance);
    }

    @Test
    public void isMemberSafe_withData() {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        fillMaps(instance);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(factory.getCount(), hz);
        }

        for (HazelcastInstance hz : instances) {
            assertLocalMemberSafeEventually(hz);
        }
        assertLocalMemberSafeEventually(instance);
    }

    private void assertLocalMemberSafeEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(instance.getPartitionService().isLocalMemberSafe());
            }
        });
    }

    private void fillMaps(HazelcastInstance instance) {
        Random random = new Random();
        for (String name : mapNames) {
            IMap<Object, Object> map = instance.getMap(name);
            int count = random.nextInt(1000) + 1;
            for (int i = 0; i < count; i++) {
                map.put(i, i);
            }
        }
    }

    private Config createConfig() {
        Config config = new Config()
                .setProperty(TCP_JOIN_PORT_TRY_COUNT.getName(), String.valueOf(getKnownPreviousVersionsCount() + 2))
                .setProperty(WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        for (int i = 0; i < mapNames.length; i++) {
            config.getMapConfig(mapNames[i]).setBackupCount(i);
        }

        return config;
    }
}
