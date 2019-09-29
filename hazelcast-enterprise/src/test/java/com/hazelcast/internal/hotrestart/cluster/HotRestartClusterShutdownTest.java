package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.NEVER;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.ALWAYS;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.SOMETIMES;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartClusterShutdownTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "clusterState:{1},nodeState:{2},reuseAddress:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ALWAYS, ClusterState.ACTIVE, NodeState.ACTIVE},
                {ALWAYS, ClusterState.FROZEN, NodeState.ACTIVE},
                {ALWAYS, ClusterState.PASSIVE, NodeState.PASSIVE},
                {SOMETIMES, ClusterState.ACTIVE, NodeState.ACTIVE},
                {SOMETIMES, ClusterState.FROZEN, NodeState.ACTIVE},
                {SOMETIMES, ClusterState.PASSIVE, NodeState.PASSIVE},
                {NEVER, ClusterState.ACTIVE, NodeState.ACTIVE},
                {NEVER, ClusterState.FROZEN, NodeState.ACTIVE},
                {NEVER, ClusterState.PASSIVE, NodeState.PASSIVE},
        });
    }

    @Parameter(1)
    public ClusterState clusterState;

    @Parameter(2)
    public NodeState nodeState;

    @Test
    public void test_clusterShutdown_respectsClusterState() {
        HazelcastInstance[] instances = startNewInstances(4);
        Address[] addresses = getAddresses(instances);

        assertInstancesJoined(instances.length, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        final Node[] nodes = getNodes(instances);

        instances[0].getCluster().shutdown();

        assertNodesShutDownEventually(nodes);

        instances = restartInstances(addresses);
        assertInstancesJoined(addresses.length, instances, nodeState, clusterState);
    }
}
