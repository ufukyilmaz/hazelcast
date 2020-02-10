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
 * 2.2.0 cluster with mixed 2.2.0 & 2.3.0 codebase nodes splits; one brain upgrades to 2.3.0 (3x 2.3.0 members), other stays
 * at 2.2.0 cluster version with 2.2.0 master (with fewer members). After communications unblocked, they remain two separate
 * clusters.
 * <p>
 * Initial cluster:
 * <ul>
 * <li>1x 2.2.0/2.2 + 4x 2.3.0/2.2 nodes</li>
 * </ul>
 * After split brain:
 * <ul>
 * <li>first half  1x 2.2.0/2.2, 1x 2.3.0/2.2</li>
 * <li>second half 3x 2.3.0/2.2, get cluster version upgrade to 3x 2.3.0/2.3</li>
 * </ul>
 * After communications restored:
 * <ul>
 * <li>1x 2.2.0/2.2 + 1x 2.3.0/2.2, separate from 3x 2.3.0/2.3 cluster</li>
 * </ul>
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCase3Test extends AbstractSplitBrainUpgradeTest {

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        instances[0] = newHazelcastInstance(factory, MEMBER_VERSION_NEXT_MINOR, config);
        instances[1] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[2] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[3] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[4] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[]{2, 3};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);

        // upgrade just the second brain so it now has more members and is upgraded to cluster version 2.3.0
        assertClusterVersion(secondBrain, CLUSTER_VERSION_NEXT_MINOR);
        getClusterService(secondBrain[0]).changeClusterVersion(CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterVersion(secondBrain, CLUSTER_VERSION_2NEXT_MINOR);

        // first brain is still at cluster 2.2.0 and has 2.2.0 codebase master
        assertClusterVersion(firstBrain, CLUSTER_VERSION_NEXT_MINOR);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        Brains brains = getBrains();
        assertClusterVersion(brains.getFirstHalf(), CLUSTER_VERSION_NEXT_MINOR);
        assertClusterVersion(brains.getSecondHalf(), CLUSTER_VERSION_2NEXT_MINOR);
    }
}
