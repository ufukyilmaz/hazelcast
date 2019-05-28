package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.ALWAYS;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.NEVER;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.SOMETIMES;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_IN_PROGRESS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartClusterStartCrashTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "reuseAddress:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                ALWAYS,
                SOMETIMES,
                NEVER
        });
    }

    private int memberListPublishPeriod = -1;

    @Test
    public void testClusterHotRestartEventListenerRegistration() {
        Address address = startAndTerminateInstance();

        final CountDownLatch latch = new CountDownLatch(1);
        final ClusterHotRestartEventListener listener = new ClusterHotRestartEventListener() {
            @Override
            public void onPrepareComplete(Collection<? extends Member> members, PartitionTableView partitionTable,
                                          boolean startWithHotRestart) {
                latch.countDown();
            }
        };

        restartInstance(address, listener);

        assertOpenEventually(latch);
    }

    @Test
    public void testMasterRestartAfterNodesJoin() {
        memberListPublishPeriod = 1;
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashMemberOnAllMembersJoin(true, crash));
        }

        restartInstances(addresses, listeners);

        assertInstancesJoinedEventually(addresses.length, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testMemberRestartAfterNodesJoin() {
        memberListPublishPeriod = 1;
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashMemberOnAllMembersJoin(false, crash));
        }

        restartInstances(addresses, listeners);

        assertInstancesJoinedEventually(addresses.length, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testByzantineFailPartitionTableAfterPartitionTablesValidated() {
        final int nodeCount = 4;
        Address[] addresses = startAndTerminateInstances(nodeCount);

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(firstCrash, nodeCount));
        }

        final HazelcastInstance[] instances = restartInstances(addresses, listeners);
        assertEquals(0, instances.length);
    }

    @Test
    public void testMemberRestartAfterReceivesLoadSuccessfulFromMaster() {
        HazelcastInstance[] instances = startNewInstances(4);

        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        final Address[] addresses = getAddresses(instances);
        terminateInstances();

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashAfterLoadCompletedStatusReceivedFromMaster(firstCrash));
        }

        restartInstances(addresses, listeners);
        assertInstancesJoinedEventually(addresses.length, NodeState.PASSIVE, ClusterState.PASSIVE);
    }

    @Override
    Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Config config = super.newConfig(listener, clusterStartPolicy);

        if (memberListPublishPeriod > 0) {
            config.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(),
                    String.valueOf(memberListPublishPeriod));
        }

        return config;
    }

    private class CrashMemberOnAllMembersJoin extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final boolean crashMaster;

        private final AtomicBoolean crash;

        private HazelcastInstance instance;

        CrashMemberOnAllMembersJoin(boolean crashMaster, AtomicBoolean crash) {
            this.crashMaster = crashMaster;
            this.crash = crash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void afterExpectedMembersJoin(Collection<? extends Member> members) {
            final Node node = getNode(instance);
            boolean shouldCrash = (crashMaster && node.isMaster()) || (!crashMaster && !node.isMaster());
            if (shouldCrash && crash.compareAndSet(false, true)) {
                Collection<HazelcastInstance> instances = getAllInstances();
                for (HazelcastInstance hz : instances) {
                    assertClusterSizeEventually(members.size(), hz);
                }

                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }
    }

    private static class ByzantineFailPartitionTableAfterPartitionTableValidationCompleted
            extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final AtomicBoolean firstCrash;

        private final Set<Member> members = Collections.newSetFromMap(new ConcurrentHashMap<Member, Boolean>());

        private final int expectedNodeCount;

        private HazelcastInstance instance;

        ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(AtomicBoolean firstCrash, final int expectedNodeCount) {
            this.firstCrash = firstCrash;
            this.expectedNodeCount = expectedNodeCount;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onDataLoadStart(Member member) {
            final Node node = getNode(instance);
            if (!node.isMaster()) {
                return;
            }
            assertOpenEventually(latch);
        }

        @Override
        public void onPartitionTableValidationResult(Member sender, boolean success) {
            final Node node = getNode(instance);
            if (!node.isMaster()) {
                return;
            }

            if (success) {
                members.add(sender);
                if (members.size() == expectedNodeCount && firstCrash.compareAndSet(false, true)) {
                    PartitionTableView partitionTableView = new PartitionTableView(
                            new PartitionReplica[PARTITION_COUNT][InternalPartition.MAX_REPLICA_COUNT], 0);
                    MemberClusterStartInfo invalidMemberClusterStartInfo
                            = new MemberClusterStartInfo(partitionTableView, LOAD_IN_PROGRESS);
                    final HotRestartIntegrationService hotRestartService =
                            (HotRestartIntegrationService) node.getNodeExtension().getInternalHotRestartService();
                    ClusterMetadataManager clusterMetadataManager = hotRestartService.getClusterMetadataManager();
                    clusterMetadataManager.receiveClusterStartInfoFromMember(sender, invalidMemberClusterStartInfo);
                    latch.countDown();
                }
            } else {
                System.err.println("Invalid ptable from " + sender);
            }
        }
    }

    private class CrashAfterLoadCompletedStatusReceivedFromMaster
            extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        CrashAfterLoadCompletedStatusReceivedFromMaster(AtomicBoolean firstCrash) {
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onHotRestartDataLoadComplete(HotRestartClusterStartStatus result, Set<String> excludedMemberUuids) {
            final Node node = getNode(instance);
            if (result == CLUSTER_START_SUCCEEDED && !node.isMaster() && firstCrash.compareAndSet(false, true)) {
                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }
    }
}
