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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/879")
public class HotRestartClusterStartCrashTest extends AbstractHotRestartClusterStartTest {

    private Future startNodeFuture;

    @After
    public void after() throws IOException {
        try {
            // Nothing to do with test's correctness.
            // Just to avoid some IO errors to be printed
            // when test finishes and deletes hot-restart folders
            // before new node's restart process completes.
            startNodeFuture.get(2, TimeUnit.MINUTES);
        } catch (Exception ignored) {
        }
        super.after();
    }

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
            listeners.put(port, new CrashMemberOnAllMembersJoin(true, crash));
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
            listeners.put(port, new CrashMemberOnAllMembersJoin(false, crash));
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
            listeners.put(port, new CrashMasterOnPartitionTableValidationComplete(firstCrash));
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

    private class CrashMemberOnAllMembersJoin extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final boolean crashMaster;

        private final AtomicBoolean crash;

        private HazelcastInstance instance;

        public CrashMemberOnAllMembersJoin(boolean crashMaster, AtomicBoolean crash) {
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
                int portToStart = node.getThisAddress().getPort();
                startNodeAfterTermination(node, portToStart);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }
    }

    private class CrashMasterOnPartitionTableValidationComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        public CrashMasterOnPartitionTableValidationComplete(AtomicBoolean firstCrash) {
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onPartitionTableValidationComplete(HotRestartClusterInitializationStatus result) {
            final Node node = getNode(instance);
            if (result == PARTITION_TABLE_VERIFIED && node.isMaster() && firstCrash.compareAndSet(false, true)) {
                int portToStart = node.getThisAddress().getPort();
                startNodeAfterTermination(node, portToStart);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

    }

    private static class ByzantineFailPartitionTableAfterPartitionTableValidationCompleted extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

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
        startNodeFuture = spawn(new Runnable() {
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
