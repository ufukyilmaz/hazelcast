package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterRollingRestartTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "clusterState:{0},partitionAssignmentType:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {ClusterState.FROZEN, PartitionAssignmentType.NEVER},
                {ClusterState.PASSIVE, PartitionAssignmentType.NEVER},
                {ClusterState.FROZEN, PartitionAssignmentType.AT_THE_END},
                {ClusterState.PASSIVE, PartitionAssignmentType.AT_THE_END},
                {ClusterState.FROZEN, PartitionAssignmentType.DURING_STARTUP},
                {ClusterState.PASSIVE, PartitionAssignmentType.DURING_STARTUP}
        });
    }

    @Parameterized.Parameter(0)
    public ClusterState clusterState;

    @Parameterized.Parameter(1)
    public PartitionAssignmentType partitionAssignmentType;

    @Test
    public void test_rollingRestart() throws Exception {
        final int nodeCount = 2;
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        final List<Integer> ports = acquirePorts(nodeCount);
        initializeFactory(ports);

        instances[0] = startInstance(ports.get(0));

        if (partitionAssignmentType == PartitionAssignmentType.DURING_STARTUP) {
            warmUpPartitions(instances[0]);
        }

        for (int i = 1; i < nodeCount; i++) {
            instances[i] = startInstance(ports.get(i));
        }

        if (partitionAssignmentType == PartitionAssignmentType.AT_THE_END) {
            warmUpPartitions(instances);
        }

        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        if (partitionAssignmentType == PartitionAssignmentType.DURING_STARTUP) {
            // this is to avoid lots of retry logs during cluster state change
            waitClusterForSafeState(instances[nodeCount - 1]);
        }

        changeClusterStateEventually(instances[0], clusterState);

        Address address = getNode(instances[0]).getThisAddress();
        instances[0].shutdown();
        instances[0] = startInstance(address.getPort());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(nodeCount, instance);
            assertEquals(clusterState, instance.getCluster().getClusterState());
        }

        if (partitionAssignmentType == PartitionAssignmentType.DURING_STARTUP) {
            // this is to avoid lots of retry logs during cluster state change
            waitClusterForSafeState(instances[0]);
        }
        changeClusterStateEventually(instances[0], ClusterState.ACTIVE);
    }

    private enum PartitionAssignmentType {
        NEVER, DURING_STARTUP, AT_THE_END
    }
}
