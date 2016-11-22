package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_2_2;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_2_3;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.VERSION_2_2_0;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.VERSION_2_3_0;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;

/**
 * Verifies that clusters operating at different cluster versions do not merge, even if their nodes initially participated
 * in the same initial cluster.
 * During split-brain, one subcluster gets its cluster version upgraded to the next minor version, while the other subcluster
 * remains at the original cluster version. After network partition is healed, the upgraded subcluster remains a separate
 * cluster from the other subcluster.
 *
 * Initial cluster:
 *                  1x2.2.0/2.2 + 2x2.3.0/2.2 nodes
 * After split brain:
 *      first half  2x2.3.0/2.2, get cluster version upgrade to 2x2.3.0/2.3
 *      second half 1x2.2.0/2.2
 * After communications restored:
 *                  1x2.2.0/2.2, separate from 2x2.3.0/2.3 cluster
 *
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCase2Test
        extends AbstractSplitBrainUpgradeTest {

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        instances[2] = newHazelcastInstance(factory, VERSION_2_2_0, config);
        instances[0] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[1] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[] {2, 1};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {
        waitAllForSafeState(firstBrain);
        // upgrade just the first brain's cluster version
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_2);
        getClusterService(firstBrain[0]).changeClusterVersion(CLUSTER_VERSION_2_3);
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_3);

        // second brain is still at 2.2.0
        assertClusterVersion(secondBrain, CLUSTER_VERSION_2_2);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws Exception {
        Brains brains = getBrains();
        assertClusterVersion(brains.getFirstHalf(), CLUSTER_VERSION_2_3);
        assertClusterVersion(brains.getSecondHalf(), CLUSTER_VERSION_2_2);
    }

}
