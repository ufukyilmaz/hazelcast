package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterShutdownTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "clusterState:{1},nodeState:{2},addressChangePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {NONE, ClusterState.ACTIVE, NodeState.ACTIVE},
                {NONE, ClusterState.FROZEN, NodeState.ACTIVE},
                {NONE, ClusterState.PASSIVE, NodeState.PASSIVE},
                {PARTIAL, ClusterState.ACTIVE, NodeState.ACTIVE},
                {PARTIAL, ClusterState.FROZEN, NodeState.ACTIVE},
                {PARTIAL, ClusterState.PASSIVE, NodeState.PASSIVE},
                {ALL, ClusterState.ACTIVE, NodeState.ACTIVE},
                {ALL, ClusterState.FROZEN, NodeState.ACTIVE},
                {ALL, ClusterState.PASSIVE, NodeState.PASSIVE}
                });
    }

    @Parameterized.Parameter(1)
    public ClusterState clusterState;

    @Parameterized.Parameter(2)
    public NodeState nodeState;

    @Test
    public void test_clusterShutdown_respectsClusterState() throws IOException, InterruptedException {
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
