package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus;
import com.hazelcast.spi.hotrestart.cluster.SendPartitionTableForValidationOperation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HotRestartClusterStartCrashTest extends AbstractHotRestartClusterStartTest {

    @Test
    public void testClusterHotRestartEventListenerRegistration() throws InterruptedException {
        final List<Integer> ports = Collections.singletonList(acquirePort());
        startAndCrashInstances(ports);

        final CountDownLatch latch = new CountDownLatch(1);
        final ClusterHotRestartEventListener listener = new ClusterHotRestartEventListener() {
            @Override
            public void onPrepareComplete(Collection<Address> members, Address[][] partitionTable,
                    boolean startWithHotRestart) {
                latch.countDown();
            }
        };

        Map<Integer, ClusterHotRestartEventListener> listeners = Collections.singletonMap(ports.get(0), listener);
        startInstances(ports, listeners);

        assertOpenEventually(latch);
    }

    @Test
    public void testMasterRestartAfterNodesJoin() throws InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new CrashMemberOnAllMembersJoin(port, ports, true, crash));
        }

        startInstances(ports, listeners);

        assertInstancesJoined(ports.size(), NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testMemberRestartAfterNodesJoin() throws InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new CrashMemberOnAllMembersJoin(port, ports, false, crash));
        }

        startInstances(ports, listeners);

        assertInstancesJoined(ports.size(), NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testMasterRestartAfterPartitionTablesValidated() throws InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new CrashMasterOnPartitionTableValidationComplete(port, firstCrash));
        }

        startInstances(ports, listeners);

        assertInstancesJoined(ports.size(), NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testByzantineFailPartitionTableAfterPartitionTablesValidated()
            throws InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(firstCrash));
        }

        final HazelcastInstance[] instances = startInstances(ports, listeners);
        assertEquals(0, instances.length);
    }

    @Test
    public void testMemberRestartAfterReceivesLoadSuccessfulFromMaster() throws InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        HazelcastInstance[] instances = startInstances(ports);

        assertInstancesJoined(ports.size(), instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);
        terminateInstances();

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new CrashAfterLoadCompletedStatusReceivedFromMaster(firstCrash));
        }

        startInstances(ports, listeners);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstancesJoined(ports.size(), NodeState.PASSIVE, ClusterState.PASSIVE);
            }
        });
    }

    private class CrashMemberOnAllMembersJoin
            extends ClusterHotRestartEventListener
            implements HazelcastInstanceAware {

        private final int portToStart;

        private final List<Integer> portsToDiscover;

        private final boolean crashMaster;

        private final AtomicBoolean crash;

        private HazelcastInstance instance;

        public CrashMemberOnAllMembersJoin(int portToStart, List<Integer> portsToDiscover, boolean crashMaster,
                                           AtomicBoolean crash) {
            this.portToStart = portToStart;
            this.portsToDiscover = portsToDiscover;
            this.crashMaster = crashMaster;
            this.crash = crash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onAllMembersJoin(Collection<Address> members) {
            final Node node = getNode(instance);
            boolean shouldCrash = crashMaster && node.isMaster() || !crashMaster && !node.isMaster();
            if (shouldCrash && crash.compareAndSet(false, true)) {
                startNodeAfterTermination(node, portToStart);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }
    }

    private class CrashMasterOnPartitionTableValidationComplete extends ClusterHotRestartEventListener
            implements HazelcastInstanceAware {

        private final int portToStart;

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        public CrashMasterOnPartitionTableValidationComplete(int portToStart, AtomicBoolean firstCrash) {
            this.portToStart = portToStart;
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onPartitionTableValidationComplete(HotRestartClusterInitializationStatus result) {
            final Node node = getNode(instance);
            if (result == PARTITION_TABLE_VERIFIED && node.isMaster() && firstCrash
                    .compareAndSet(false, true)) {
                startNodeAfterTermination(node, portToStart);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

    }

    private static class ByzantineFailPartitionTableAfterPartitionTableValidationCompleted
            extends ClusterHotRestartEventListener
            implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        public ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(AtomicBoolean firstCrash) {
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onPartitionTableValidationComplete(HotRestartClusterInitializationStatus result) {
            final Node node = getNode(instance);
            if (result == PARTITION_TABLE_VERIFIED && !node.isMaster()
                    && firstCrash.compareAndSet(false, true)) {
                InternalOperationService operationService = node.getNodeEngine().getOperationService();
                Operation op = new SendPartitionTableForValidationOperation(null);
                operationService.send(op, node.getMasterAddress());
            }
        }

    }

    private class CrashAfterLoadCompletedStatusReceivedFromMaster extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        public CrashAfterLoadCompletedStatusReceivedFromMaster(AtomicBoolean firstCrash) {
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onHotRestartDataLoadComplete(HotRestartClusterInitializationStatus result) {
            final Node node = getNode(instance);
            final int portToStart = node.getThisAddress().getPort();
            if (result == VERIFICATION_AND_LOAD_SUCCEEDED && !node.isMaster()
                    && firstCrash.compareAndSet(false, true)) {
                startNodeAfterTermination(node, portToStart);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

    }

    private void startNodeAfterTermination(final Node node, final int portToStart) {
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    assertNodeStateEventually(node, NodeState.SHUT_DOWN);
                    System.out.println(">>> Starting " + portToStart + " again!");
                    startInstance(portToStart);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
