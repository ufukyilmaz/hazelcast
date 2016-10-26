package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterRollingRestartTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "clusterState:{1},addressChangePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {NONE, ClusterState.FROZEN},
                {NONE, ClusterState.PASSIVE},
                {PARTIAL, ClusterState.FROZEN},
                {PARTIAL, ClusterState.PASSIVE},
                {ALL, ClusterState.FROZEN},
                {ALL, ClusterState.PASSIVE},
        });
    }

    @Parameterized.Parameter(1)
    public ClusterState clusterState;

    private int nodeCount = 3;

    @Test
    public void test_rollingRestart_withoutPartitionAssignment() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = startNewInstance();
        }

        rollingRestartInstances(instances, false);
    }

    @Test
    public void test_rollingRestart_withPartitionAssignment_atTheEnd() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = startNewInstance();
        }

        warmUpPartitions(instances);

        rollingRestartInstances(instances, true);
    }

    @Test
    public void test_rollingRestart_withPartitionAssignment_duringStartup() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        instances[0] = startNewInstance();
        warmUpPartitions(instances[0]);

        for (int i = 1; i < nodeCount; i++) {
            instances[i] = startNewInstance();
        }

        rollingRestartInstances(instances, true);
    }

    private void rollingRestartInstances(HazelcastInstance[] instances, boolean invokePartitionOps) {
        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        // this is to avoid lots of retry logs during cluster state change
        waitClusterForSafeState(instances[nodeCount - 1]);

        changeClusterStateEventually(instances[0], clusterState);

        for (int i = 0; i < nodeCount; i++) {
            int k = i % instances.length;
            Address address = getAddress(instances[k]);
            instances[k].getLifecycleService().terminate();
            instances[k] = restartInstance(address);

            if (invokePartitionOps) {
                invokeDummyOperationOnAllPartitions(instances);
            }
        }

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(nodeCount, instance);
            assertEquals(clusterState, instance.getCluster().getClusterState());
        }

    }
}
