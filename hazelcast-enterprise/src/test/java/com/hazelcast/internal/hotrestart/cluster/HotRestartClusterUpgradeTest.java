package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertNodesVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartClusterUpgradeTest extends AbstractHotRestartClusterStartTest {

    private static final MemberVersion MEMBER_VERSION;
    private static final MemberVersion NEXT_MINOR_MEMBER_VERSION;
    private static final MemberVersion PREVIOUS_MINOR_MEMBER_VERSION;
    private static final MemberVersion NEXT_MAJOR_MEMBER_VERSION;

    static {
        MemberVersion currentVersion = MemberVersion.of(CURRENT_CLUSTER_VERSION.toString());
        MemberVersion nextMinorVersion = MemberVersion.of(currentVersion.getMajor(),
                currentVersion.getMinor() + 1,
                currentVersion.getPatch());
        MemberVersion previousMinorVersion = currentVersion.getMinor() == 0 ? null
                : MemberVersion.of(currentVersion.getMajor(), currentVersion.getMinor() - 1, currentVersion.getPatch());
        MemberVersion nextMajorVersion = MemberVersion.of(currentVersion.getMajor() + 1, 0, 0);

        MEMBER_VERSION = currentVersion;
        NEXT_MINOR_MEMBER_VERSION = nextMinorVersion;
        PREVIOUS_MINOR_MEMBER_VERSION = previousMinorVersion;
        NEXT_MAJOR_MEMBER_VERSION = nextMajorVersion;
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated, upgraded to codebase version V+1 then restarted
    // the cluster should restart and still operate at cluster version V
    @Test
    public void test_hotRestart_afterRollingUpgrade_toNextMinorVersion_withOriginalClusterVersion() {
        HazelcastInstance[] instances = startNewInstances(2, FULL_RECOVERY_ONLY, MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
        shutdownCluster();

        instances = restartInstances(addresses, NEXT_MINOR_MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertNodesVersion(instances, NEXT_MINOR_MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
    }

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated, upgraded to codebase version V+1 then restarted
    // cluster restarts and operates at cluster version V
    // cluster version is upgraded to V+1
    // nodes are terminated
    // cluster should restart and operate at cluster version V+1
    @Test
    public void test_hotRestart_afterRollingUpgrade_toNextMinorVersion_withUpgradedClusterVersion() {
        HazelcastInstance[] instances = startNewInstances(2, FULL_RECOVERY_ONLY, MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
        shutdownCluster();

        instances = restartInstances(addresses, NEXT_MINOR_MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertNodesVersion(instances, NEXT_MINOR_MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());

        // upgrade cluster version to next minor version
        getClusterService(instances[0]).changeClusterVersion(NEXT_MINOR_MEMBER_VERSION.asVersion());
        assertClusterVersion(instances, NEXT_MINOR_MEMBER_VERSION.asVersion());
        shutdownCluster();

        instances = restartInstances(addresses, NEXT_MINOR_MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertClusterVersion(instances, NEXT_MINOR_MEMBER_VERSION.asVersion());
    }

    // cluster of nodes starts at codebase/cluster version V
    // nodes are terminated
    //      one node is upgraded to compatible codebase version V+1
    //      other node is downgraded to incompatible codebase version V-1
    // the cluster restarts with partial restart policy and operates at cluster version V with just one member
    // V-1 member does not start because its version is incompatible with the stored cluster version
    @Test
    public void test_hotRestartPartialSucceeds_whenOneMember_hasIncompatibleCodebaseVersion() {
        assumeNotNull(PREVIOUS_MINOR_MEMBER_VERSION);
        final HazelcastInstance[] instances = startNewInstances(2, FULL_RECOVERY_ONLY, MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        final Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
        shutdownCluster();

        instances[0] = restartInstance(addresses[0], null,
                HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE,
                NEXT_MINOR_MEMBER_VERSION);

        expectedException.expect(HotRestartException.class);
        instances[1] = restartInstance(addresses[1], PREVIOUS_MINOR_MEMBER_VERSION);

        // only one node starts in the final cluster
        assertInstancesJoined(1, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
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
        assumeNotNull(PREVIOUS_MINOR_MEMBER_VERSION);
        final HazelcastInstance[] instances = startNewInstances(2, FULL_RECOVERY_ONLY, MEMBER_VERSION);
        assertInstancesJoined(2, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);

        final Address[] addresses = new Address[]{getAddress(instances[0]), getAddress(instances[1])};
        assertNodesVersion(instances, MEMBER_VERSION);
        assertClusterVersion(instances, MEMBER_VERSION.asVersion());
        shutdownCluster();

        // instance with codebase version minor-1 will fail due to incompatibility with cluster version
        expectedException.expect(HotRestartException.class);
        instances[1] = restartInstance(addresses[1], PREVIOUS_MINOR_MEMBER_VERSION);

        // instance with codebase minor+1 will fail due to full recovery policy and member being unavailable
        expectedException.expect(IllegalStateException.class);
        instances[0] = restartInstance(addresses[0], NEXT_MINOR_MEMBER_VERSION);
    }

    @Test
    public void test_hotRestartFails_whenNodeCodebaseVersionIncompatibleWithPersistedClusterVersion() {
        HazelcastInstance instance = startNewInstance();
        Address address = getAddress(instance);
        warmUpPartitions(instance);
        shutdownCluster();

        expectedException.expect(HotRestartException.class);
        restartInstance(address, NEXT_MAJOR_MEMBER_VERSION);
    }

    private void shutdownCluster() {
        final Collection<HazelcastInstance> instances = getAllInstances();
        assertThat(instances, Matchers.not(Matchers.<HazelcastInstance>empty()));
        waitAllForSafeState(instances);

        final HazelcastInstance hz = instances.iterator().next();
        // For 3.9 compatibility:
        // Ensure that all members have the same member-list version after rolling upgrade.
        // See MembershipManager.scheduleMemberListVersionIncrement().
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int memberListVersion = getNode(hz).getClusterService().getMemberListVersion();
                for (HazelcastInstance instance : instances) {
                    assertEquals(memberListVersion, getNode(instance).getClusterService().getMemberListVersion());
                }
            }
        });
        hz.getCluster().shutdown();
    }
}
