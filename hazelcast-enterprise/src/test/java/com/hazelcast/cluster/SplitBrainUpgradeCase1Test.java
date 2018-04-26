package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_2NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_NEXT_MINOR;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertNodesVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that the nodes' codebase version does not affect the ability to merge clusters which are at the same cluster version.
 * <p>
 * While cluster is split, upgrade a node's codebase version. Both sub-clusters remain at the same cluster version. After
 * network partition is healed, the sub-clusters are able to merge again.
 * <p>
 * Initial cluster:
 * <ul>
 * <li>1x 2.2.0/2.2 + 3x 2.3.0/2.2 nodes</li>
 * </ul>
 * After split brain:
 * <ul>
 * <li>first half  1x 2.2.0/2.2 + 1x 2.3.0/2.2 --> upgrade codebase version on 1st instance --> 2x 2.3.0/2.2</li>
 * <li>second half 2x 2.3.0/2.2</li>
 * </ul>
 * After communications restored:
 * <ul>
 * <li>4x 2.3.0/2.2</li>
 * </ul>
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SplitBrainUpgradeCase1Test extends AbstractSplitBrainUpgradeTest {

    private HazelcastInstance[] instances;

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        instances = new HazelcastInstance[clusterSize];
        instances[0] = newHazelcastInstance(factory, MEMBER_VERSION_NEXT_MINOR, config);
        instances[1] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[2] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[3] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[]{2, 2};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        // wait for sub-clusters to settle
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);

        // first brain is at cluster version 2.2.0
        assertClusterVersion(firstBrain, CLUSTER_VERSION_NEXT_MINOR);

        // upgrade just the first node of first brain (single 2.2.0 node to 2.3.0)
        firstBrain[0].shutdown();
        waitAllForSafeState(firstBrain);
        assertEquals(CLUSTER_VERSION_NEXT_MINOR, firstBrain[1].getCluster().getClusterVersion());
        instances[0] = createHazelcastInstanceInBrain(0, MEMBER_VERSION_2NEXT_MINOR);
        firstBrain[0] = instances[0];

        // make sure we are still at 2.2 cluster version
        assertClusterVersion(firstBrain, CLUSTER_VERSION_NEXT_MINOR);
        assertClusterVersion(secondBrain, CLUSTER_VERSION_NEXT_MINOR);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        assertClusterVersion(instances, CLUSTER_VERSION_NEXT_MINOR);
        assertClusterSize(instances.length, instances[1]);
        assertNodesVersion(instances, MEMBER_VERSION_2NEXT_MINOR);
    }
}
