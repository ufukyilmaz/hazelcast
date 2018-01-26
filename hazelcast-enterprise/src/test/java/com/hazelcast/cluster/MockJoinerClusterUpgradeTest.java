package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static com.hazelcast.test.TestClusterUpgradeUtils.upgradeClusterMembers;
import static org.junit.Assert.assertEquals;

/**
 * Creates a cluster, then change cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class MockJoinerClusterUpgradeTest extends AbstractClusterUpgradeTest {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(10);

    @Override
    HazelcastInstance createHazelcastInstance(MemberVersion version, Config config) {
        return newHazelcastInstance(factory, version, config);
    }

    @Override
    void upgradeInstances(HazelcastInstance[] instances, MemberVersion version, Config config) {
        upgradeClusterMembers(factory, instances, version, config);
    }

    @Override
    void assertNodesVersion(MemberVersion version) {
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }

    @After
    public void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }
}
