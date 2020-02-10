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
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 2.2 cluster with mixed 2.2.0 & 2.3.0 codebase nodes splits; one brain upgrades to 2.3 (only 2.3.0 members), other stays
 * at 2.2 cluster version with 2.3.0 master. After communications unblocked, two separate clusters remain, as cluster at
 * version 2.2 should not merge with 2.3 (would result in "sneaky upgrade" of 2.3.0-master and kicking 2.2.0 member out).
 * <p>
 * Initial cluster:
 * <ul>
 * <li>2x 2.2.0/2.2 + 4x 2.3.0/2.2 nodes</li>
 * </ul>
 * After split brain:
 * <ul>
 * <li>first half  2x 2.2.0/2.2, 1x 2.3.0/2.2, shutdown 1x 2.2.0/2.2, 2.3.0/2.2 becomes master of first half</li>
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
public class SplitBrainUpgradeCase4Test extends AbstractSplitBrainUpgradeTest {

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];

        // this starts as the master. will be shutdown after split brain to allow 2.3.0 at [1] to become master
        instances[0] = newHazelcastInstance(factory, MEMBER_VERSION_NEXT_MINOR, config);
        instances[1] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);

        // this is the one whose behaviour we are inspecting
        instances[2] = newHazelcastInstance(factory, MEMBER_VERSION_NEXT_MINOR, config);
        instances[3] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[4] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        instances[5] = newHazelcastInstance(factory, MEMBER_VERSION_2NEXT_MINOR, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[]{3, 3};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);
        // upgrade just the second brain so it now has 3 members and is upgraded to cluster version 2.3.0
        assertClusterVersion(secondBrain, CLUSTER_VERSION_NEXT_MINOR);
        getClusterService(secondBrain[0]).changeClusterVersion(CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterVersion(secondBrain, CLUSTER_VERSION_2NEXT_MINOR);

        // first brain is still at cluster 2.2.0 and has 2.2.0 codebase master
        assertClusterVersion(firstBrain, CLUSTER_VERSION_NEXT_MINOR);
        firstBrain[0].shutdown();
        waitAllForSafeState(firstBrain);
        assertEquals(CLUSTER_VERSION_NEXT_MINOR, firstBrain[1].getCluster().getClusterVersion());
        assertEquals(CLUSTER_VERSION_NEXT_MINOR, firstBrain[2].getCluster().getClusterVersion());
        // make sure the 2.3.0 node is now master of the first brain
        assertTrue(getNode(firstBrain[1]).isMaster());
        // and the two nodes remaining in first brain are still on cluster version 2.2.0
        assertEquals(CLUSTER_VERSION_NEXT_MINOR, firstBrain[1].getCluster().getClusterVersion());
        assertEquals(CLUSTER_VERSION_NEXT_MINOR, firstBrain[2].getCluster().getClusterVersion());
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        // after network partition is healed:
        // - nodes 3,4 & 5 are in a single cluster at 2.3.0
        // - nodes 1 & 2 are in another cluster at 2.2.0
        HazelcastInstance[] instancesAtClusterVersion_2_3_0 = new HazelcastInstance[]{
                instances[3], instances[4], instances[5],
        };
        HazelcastInstance[] instancesAtClusterVersion_2_2_0 = new HazelcastInstance[]{
                instances[1], instances[2],
        };
        assertClusterVersion(instancesAtClusterVersion_2_3_0, CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterSize(3, instances[3]);

        // so it attempted to merge, was reset and then failed to join
        assertClusterVersion(instancesAtClusterVersion_2_2_0, CLUSTER_VERSION_NEXT_MINOR);
        assertClusterSize(2, instances[2]);
    }
}
