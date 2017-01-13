package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertNodesVersion;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterUpgradeTest extends AbstractHotRestartClusterStartTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated, upgraded to codebase version V+1 then restarted
    // the cluster should restart and still operate at cluster version V
    @Test
    public void test_hotRestart_afterRollingUpgrade_toNextMinorVersion_withOriginalClusterVersion() {
        HazelcastInstance[] instances = startNewInstances(2);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        MemberVersion originalCodebaseVersion = getNode(instances[0]).getVersion();
        MemberVersion nextMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() + 1,
                originalCodebaseVersion.getPatch());

        Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, originalCodebaseVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
        shutdownCluster();

        instances = restartInstances(addresses, nextMinorVersion);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertNodesVersion(instances, nextMinorVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
    }

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated, upgraded to codebase version V+1 then restarted
    // cluster restarts and operates at cluster version V
    // cluster version is upgraded to V+1
    // nodes are terminated
    // cluster should restart and operate at cluster version V+1
    @Test
    public void test_hotRestart_afterRollingUpgrade_toNextMinorVersion_withUpgradedClusterVersion() {
        HazelcastInstance[] instances = startNewInstances(2);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        MemberVersion originalCodebaseVersion = getNode(instances[0]).getVersion();
        MemberVersion nextMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() + 1,
                originalCodebaseVersion.getPatch());

        Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, originalCodebaseVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
        shutdownCluster();

        instances = restartInstances(addresses, nextMinorVersion);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertNodesVersion(instances, nextMinorVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());

        // upgrade cluster version to next minor version
        getClusterService(instances[0]).changeClusterVersion(nextMinorVersion.asClusterVersion());
        assertClusterVersion(instances, nextMinorVersion.asClusterVersion());
        shutdownCluster();

        instances = restartInstances(addresses, nextMinorVersion);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertClusterVersion(instances, nextMinorVersion.asClusterVersion());
    }

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated
    //      one node is upgraded to compatible codebase version V+1
    //      other node is downgraded to incompatible codebase version V-1
    // the cluster restarts with partial restart policy and operates at cluster version V with just one member
    // V-1 member does not start because its version is incompatible with the stored cluster version
    @Test
    public void test_hotRestartPartialSucceeds_whenOneMember_hasIncompatibleCodebaseVersion() {
        final HazelcastInstance[] instances = startNewInstances(2);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        MemberVersion originalCodebaseVersion = getNode(instances[0]).getVersion();
        MemberVersion nextMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() + 1,
                originalCodebaseVersion.getPatch());

        MemberVersion previousMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() - 1,
                originalCodebaseVersion.getPatch());

        final Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, originalCodebaseVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
        shutdownCluster();

        instances[0] = restartInstance(addresses[0], null,
                HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE,
                nextMinorVersion);

        expectedException.expect(HotRestartException.class);
        instances[1] = restartInstance(addresses[1], previousMinorVersion);

        // only one node starts in the final cluster
        assertInstancesJoined(1, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
    }

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated
    //      one node is upgraded to compatible codebase version V+1
    //      other node is downgraded to incompatible codebase version V-1
    // the cluster members are restarted with full recovery restart policy
    // V-1 member does not start because its version is incompatible with the stored cluster version
    // V+1 member does not start because its missing one member of the cluster
    @Test
    public void test_hotRestartFullRecoveryFails_whenOneMember_hasIncompatibleCodebaseVersion() {
        final HazelcastInstance[] instances = startNewInstances(2);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        MemberVersion originalCodebaseVersion = getNode(instances[0]).getVersion();
        final MemberVersion nextMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() + 1,
                originalCodebaseVersion.getPatch());

        final MemberVersion previousMinorVersion = MemberVersion.of(originalCodebaseVersion.getMajor(),
                originalCodebaseVersion.getMinor() - 1,
                originalCodebaseVersion.getPatch());

        final Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, originalCodebaseVersion);
        assertClusterVersion(instances, originalCodebaseVersion.asClusterVersion());
        shutdownCluster();

        // instance with codebase version minor-1 will fail due to incompatibility with cluster version
        expectedException.expect(HotRestartException.class);
        instances[1] = restartInstance(addresses[1], previousMinorVersion);

        // instance with codebase minor+1 will fail due to full recovery policy and member being unavailable
        expectedException.expect(IllegalStateException.class);
        instances[0] = restartInstance(addresses[0], nextMinorVersion);
    }

    @Test
    public void test_hotRestartFails_whenNodeCodebaseVersionIncompatibleWithPersistedClusterVersion() {
        HazelcastInstance instance = startNewInstance();
        Address address = getAddress(instance);
        warmUpPartitions(instance);
        MemberVersion originalCodebaseVersion = getNode(instance).getVersion();
        shutdownCluster();

        MemberVersion nextMajorVersion = MemberVersion.of(originalCodebaseVersion.getMajor() + 1,
                originalCodebaseVersion.getMinor(),
                originalCodebaseVersion.getPatch());

        expectedException.expect(HotRestartException.class);
        restartInstance(address, nextMajorVersion);
    }

    private void shutdownCluster() {
        Collection<HazelcastInstance> instances = getAllInstances();
        assertThat(instances, Matchers.not(Matchers.<HazelcastInstance>empty()));
        waitAllForSafeState(instances);
        instances.iterator().next().getCluster().shutdown();
    }
}
