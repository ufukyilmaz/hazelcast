package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_CURRENT;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_CURRENT;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_NEXT_MINOR;
import static com.hazelcast.internal.cluster.impl.ClusterVersionAutoUpgradeHelper.PROP_AUTO_UPGRADE_ENABLED;
import static com.hazelcast.internal.cluster.impl.ClusterVersionAutoUpgradeHelper.PROP_AUTO_UPGRADE_MIN_CLUSTER_SIZE;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AutoUpgradeSplitBrainTest extends AbstractSplitBrainUpgradeTest {

    private HazelcastInstance[] instances;
    private static final int NUMBER_OF_INSTANCES = 4;

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize * 2);

        instances = new HazelcastInstance[clusterSize];

        // start cluster with current version.
        for (int i = 0; i < NUMBER_OF_INSTANCES; i++) {
            instances[i] = newHazelcastInstance(factory, MEMBER_VERSION_CURRENT, config);
        }

        return instances;
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.setProperty(PROP_AUTO_UPGRADE_ENABLED, "true");
        config.setProperty(PROP_AUTO_UPGRADE_MIN_CLUSTER_SIZE, String.valueOf(NUMBER_OF_INSTANCES));
        return config;
    }

    @Override
    protected int[] brains() {
        return new int[]{2, 2};
    }

    @Override
    protected void onAfterSplitBrainCreated(final HazelcastInstance[] firstBrain, final HazelcastInstance[] secondBrain) {
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);

        // upgrade both brains
        int i;
        for (i = 0; i < firstBrain.length; i++) {
            firstBrain[i].shutdown();
            instances[i] = createHazelcastInstanceInBrain(0, MEMBER_VERSION_NEXT_MINOR);
            firstBrain[i] = instances[i];
        }

        for (int j = 0; j < secondBrain.length; j++) {
            secondBrain[j].shutdown();
            instances[i + j] = createHazelcastInstanceInBrain(1, MEMBER_VERSION_NEXT_MINOR);
            secondBrain[j] = instances[i + j];
        }

        // not expecting to upgrade cluster version due to the desired cluster size.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(firstBrain, CLUSTER_VERSION_CURRENT);
            }
        }, 5);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(secondBrain, CLUSTER_VERSION_CURRENT);
            }
        }, 5);
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(instances, CLUSTER_VERSION_NEXT_MINOR);
            }
        });
    }
}
