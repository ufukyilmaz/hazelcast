package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import com.hazelcast.version.MemberVersion;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static com.hazelcast.test.TestClusterUpgradeUtils.upgradeClusterMembers;
import static org.junit.Assert.assertEquals;

/**
 * Creates a cluster, then change cluster version.
 */
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, SerializationSamplesExcluded.class, ParallelTest.class})
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

}
