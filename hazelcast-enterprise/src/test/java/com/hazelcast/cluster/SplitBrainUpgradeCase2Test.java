package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_2NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_2NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_NEXT_MINOR;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;

/**
 * Verifies that clusters operating at different cluster versions do not merge, even if their nodes initially participated
 * in the same initial cluster.
 * <p>
 * During split-brain, one sub-cluster gets its cluster version upgraded to the next minor version, while the other sub-cluster
 * remains at the original cluster version. After network partition is healed, the upgraded sub-cluster remains a separate
 * cluster from the other sub-cluster.
 * <p>
 * Initial cluster:
 * <ul>
 * <li>1x 2.2.0/2.2 + 2x 2.3.0/2.2 nodes</li>
 * </ul>
 * After split brain:
 * <ul>
 * <li>first half  2x 2.3.0/2.2, get cluster version upgrade to 2x 2.3.0/2.3</li>
 * <li>second half 1x 2.2.0/2.2</li>
 * </ul>
 * After communications restored:
 * <ul>
 * <li>1x 2.2.0/2.2, separate from 2x 2.3.0/2.3 cluster</li>
 * </ul>
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCase2Test extends AbstractSplitBrainUpgradeTest {

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        instances[2] = newHazelcastInstance(factory, MEMBER_VERSION_NEXT_MINOR, config);
        instances[0] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[1] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[]{2, 1};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        waitAllForSafeState(firstBrain);

        // upgrade just the first brain's cluster version
        assertClusterVersion(firstBrain, CLUSTER_VERSION_NEXT_MINOR);
        getClusterService(firstBrain[0]).changeClusterVersion(CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2NEXT_MINOR);

        // second brain is still at 2.2.0
        assertClusterVersion(secondBrain, CLUSTER_VERSION_NEXT_MINOR);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        Brains brains = getBrains();
        assertClusterVersion(brains.getFirstHalf(), CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterVersion(brains.getSecondHalf(), CLUSTER_VERSION_NEXT_MINOR);
    }
}
