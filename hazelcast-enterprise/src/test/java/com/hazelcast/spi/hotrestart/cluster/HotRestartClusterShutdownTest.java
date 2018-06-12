package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
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

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterShutdownTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "clusterState:{1},nodeState:{2},addressChangePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {NONE, ClusterState.ACTIVE, NodeState.ACTIVE},
                {NONE, ClusterState.FROZEN, NodeState.ACTIVE},
                {NONE, ClusterState.PASSIVE, NodeState.PASSIVE},
                {PARTIAL, ClusterState.ACTIVE, NodeState.ACTIVE},
                {PARTIAL, ClusterState.FROZEN, NodeState.ACTIVE},
                {PARTIAL, ClusterState.PASSIVE, NodeState.PASSIVE},
                {ALL, ClusterState.ACTIVE, NodeState.ACTIVE},
                {ALL, ClusterState.FROZEN, NodeState.ACTIVE},
                {ALL, ClusterState.PASSIVE, NodeState.PASSIVE},
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
