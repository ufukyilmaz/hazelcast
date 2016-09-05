package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HotRestartClusterShutdownTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "clusterState:{0},nodeState:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {ClusterState.ACTIVE, NodeState.ACTIVE},
                {ClusterState.FROZEN, NodeState.ACTIVE},
                {ClusterState.PASSIVE, NodeState.PASSIVE},
                });
    }

    @Parameterized.Parameter
    public ClusterState clusterState;

    @Parameterized.Parameter(1)
    public NodeState nodeState;

    @Test
    public void test_clusterShutdown_respectsClusterState() throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        HazelcastInstance[] instances = startInstances(ports);

        assertInstancesJoined(ports.size(), instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        final Node[] nodes = getNodes(instances);

        instances[0].getCluster().shutdown();

        assertNodesShutDownEventually(nodes);

        instances = startInstances(ports);
        assertInstancesJoined(4, instances, nodeState, clusterState);
    }

}
