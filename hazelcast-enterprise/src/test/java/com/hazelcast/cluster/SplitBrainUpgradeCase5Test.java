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
import static org.junit.Assert.assertEquals;

/**
 * 2.2 cluster with mixed 2.2.0 & 2.3.0 codebase nodes splits; one brain upgrades cluster version to 2.3 (3x2.3.0 members),
 * other subcluster's 2.2.0 node is upgraded to 2.3.0 node codebase version and cluster version is also upgraded to 2.3.
 * After comms unblocked, they merge at the upgraded cluster version.
 *
 * Initial cluster:
 *                  1x2.2.0/2.2 + 4x2.3.0/2.2 nodes
 * After split brain:
 *      first half  1x2.2.0/2.2 gets upgraded to 2.3.0/2.2, 1x2.3.0/2.2 --> cluster version upgrade to 2.3
 *      second half 3x2.3.0/2.2, get cluster version upgrade to 3x2.3.0/2.3
 * After communications restored:
 *                  5x2.3.0/2.3
 *
 * Version notation: NODE.CODEBASE.VERSION/CLUSTER.VERSION. For example, 2.3.0/2.2 denotes a node with codebase version
 * 2.3.0, operating at 2.2 cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCase5Test
        extends AbstractSplitBrainUpgradeTest {

    HazelcastInstance[] instances;

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);
        instances = new HazelcastInstance[clusterSize];
        instances[0] = newHazelcastInstance(factory, VERSION_2_2_0, config);
        instances[1] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[2] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[3] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        instances[4] = newHazelcastInstance(factory, VERSION_2_3_0, config);
        return instances;
    }

    @Override
    protected int[] brains() {
        return new int[] {2, 3};
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);
        // first brain is still at cluster 2.2.0 and has 2.2.0 codebase master
        // > shutdown 2.2.0 node
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_2);
        firstBrain[0].shutdown();
        waitAllForSafeState(firstBrain);
        assertEquals(CLUSTER_VERSION_2_2, firstBrain[1].getCluster().getClusterVersion());
        // > upgrade just the first node of first brain (single 2.2.0 node to 2.3.0)
        instances[0] = newHazelcastInstance(factory, VERSION_2_3_0, config());
        firstBrain[0] = instances[0];
        // > assert first brain is still at 2.2.0 cluster version
        assertClusterVersion(firstBrain, CLUSTER_VERSION_2_2);
        // > upgrade first brain to 2.3.0 cluster version
        getClusterService(firstBrain[1]).changeClusterVersion(CLUSTER_VERSION_2_3);

        // upgrade second brain's cluster version to 2.3.0
        assertClusterVersion(secondBrain, CLUSTER_VERSION_2_2);
        getClusterService(secondBrain[0]).changeClusterVersion(CLUSTER_VERSION_2_3);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws Exception {
        assertClusterVersion(instances, CLUSTER_VERSION_2_3);
    }

}
