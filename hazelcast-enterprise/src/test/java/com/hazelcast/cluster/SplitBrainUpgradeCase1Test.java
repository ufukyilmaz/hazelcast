package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_2_2;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.VERSION_2_2_0;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.VERSION_2_3_0;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertNodesVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static org.junit.Assert.assertEquals;

/**
 * Verify that nodes' codebase version does not affect the ability to merge clusters which are at the same cluster version.
 * While cluster is split, upgrade a node's codebase version. Both subclusters remain at the same cluster version. After
 * network partition is healed, the subclusters are able to merge again.
 *
 * Initial cluster:
 *                  1x2.2.0/2.2 + 3x2.3.0/2.2 nodes
 * After split brain:
 *      first half  1x2.2.0/2.2 + 1x2.3.0/2.2 --> upgrade codebase version on 1st instance --> 2x2.3.0/2.2
 *      second half 2x2.3.0/2.2
 * After communications restored:
 *                  4x2.3.0/2.2
 *
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCase1Test extends AbstractSplitBrainUpgradeTest {

    private HazelcastInstance[] instances;

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        instances = new HazelcastInstance[clusterSize];
        instances[0] = newHazelcastInstance(factory, VERSION_2_2_0, config);
        instances[1] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[2] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[3] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[] {2, 2};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {
        // wait for subclusters to settle
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);
        // first brain is at cluster version 2.2.0
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_2);
        // upgrade just the first node of first brain (single 2.2.0 node to 2.3.0)
        firstBrain[0].shutdown();
        waitAllForSafeState(firstBrain);
        assertEquals(CLUSTER_VERSION_2_2, firstBrain[1].getCluster().getClusterVersion());
        // upgrade just the first node of first brain (single 2.2.0 node to 2.3.0)
        instances[0] = createHazelcastInstanceInBrain(0, VERSION_2_3_0);
        firstBrain[0] = instances[0];
        // make sure we are still at 2.2 cluster version
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_2);

        // second brain is still at 2.2
        assertClusterVersion(secondBrain, CLUSTER_VERSION_2_2);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws Exception {
        assertClusterVersion(instances, CLUSTER_VERSION_2_2);
        assertClusterSize(instances.length, instances[1]);
        assertNodesVersion(instances, VERSION_2_3_0);
    }

}
