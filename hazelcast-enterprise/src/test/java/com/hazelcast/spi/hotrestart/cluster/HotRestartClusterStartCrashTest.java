package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterStartCrashTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[] {NONE, PARTIAL, ALL});
    }

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
    public void testMasterRestartAfterNodesJoin() throws InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashMemberOnAllMembersJoin(true, crash));
        }

        restartInstances(addresses, listeners);

        assertInstancesJoined(addresses.length, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testMemberRestartAfterNodesJoin() throws InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean crash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashMemberOnAllMembersJoin(false, crash));
        }

        restartInstances(addresses, listeners);

        assertInstancesJoined(addresses.length, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testMasterRestartAfterPartitionTablesValidated() throws InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new CrashMasterOnPartitionTableValidationComplete(firstCrash));
        }

        restartInstances(addresses, listeners);

        assertInstancesJoined(addresses.length, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testByzantineFailPartitionTableAfterPartitionTablesValidated()
            throws InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        final AtomicBoolean firstCrash = new AtomicBoolean(false);
        final Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(firstCrash));
        }

        final HazelcastInstance[] instances = restartInstances(addresses, listeners);
        assertEquals(0, instances.length);
    }

    @Test
    public void testMemberRestartAfterReceivesLoadSuccessfulFromMaster() throws InterruptedException {
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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstancesJoined(addresses.length, NodeState.PASSIVE, ClusterState.PASSIVE);
            }
        });
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
        public void onAllMembersJoin(Collection<? extends Member> members) {
            final Node node = getNode(instance);
            boolean shouldCrash = crashMaster && node.isMaster() || !crashMaster && !node.isMaster();
            if (shouldCrash && crash.compareAndSet(false, true)) {
                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }
    }

    private class CrashMasterOnPartitionTableValidationComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        CrashMasterOnPartitionTableValidationComplete(AtomicBoolean firstCrash) {
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
                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

    }

    private static class ByzantineFailPartitionTableAfterPartitionTableValidationCompleted extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean firstCrash;

        private HazelcastInstance instance;

        ByzantineFailPartitionTableAfterPartitionTableValidationCompleted(AtomicBoolean firstCrash) {
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

        CrashAfterLoadCompletedStatusReceivedFromMaster(AtomicBoolean firstCrash) {
            this.firstCrash = firstCrash;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onHotRestartDataLoadComplete(HotRestartClusterInitializationStatus result) {
            final Node node = getNode(instance);
            if (result == VERIFICATION_AND_LOAD_SUCCEEDED && !node.isMaster() && firstCrash.compareAndSet(false, true)) {
                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

    }

    private void startNodeAfterTermination(final Node node) {
        startNodeFuture = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    Address address = node.getThisAddress();
                    assertNodeStateEventually(node, NodeState.SHUT_DOWN);
                    // we can't change address anymore after cluster restart is initiated
                    addressChangePolicy = NONE;
                    restartInstance(address);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
