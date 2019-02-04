package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.NEVER;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.ALWAYS;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.SOMETIMES;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterRollingRestartTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "clusterState:{1},reuseAddress:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ALWAYS, FROZEN},
                {ALWAYS, PASSIVE},
                {SOMETIMES, FROZEN},
                {SOMETIMES, PASSIVE},
                {NEVER, FROZEN},
                {NEVER, PASSIVE},
        });
    }

    @Parameter(1)
    public ClusterState clusterState;

    private int nodeCount = 3;

    @Test
    public void test_rollingRestart_withoutPartitionAssignment() {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = startNewInstance();
        }

        rollingRestartInstances(instances, false);
    }

    @Test
    public void test_rollingRestart_withPartitionAssignment_atTheEnd() {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = startNewInstance();
        }

        warmUpPartitions(instances);

        rollingRestartInstances(instances, true);
    }

    @Test
    public void test_rollingRestart_withPartitionAssignment_duringStartup() {
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
            assertClusterState(clusterState, instance);
        }
    }
}
