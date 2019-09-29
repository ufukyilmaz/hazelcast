package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.nio.Address;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreRegistry;
import com.hazelcast.internal.hotrestart.RecordDataSink;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.ALWAYS;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.NEVER;
import static com.hazelcast.internal.hotrestart.cluster.AbstractHotRestartClusterStartTest.ReuseAddress.SOMETIMES;
import static com.hazelcast.internal.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ForceStartTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "reuseAddress:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                ALWAYS,
                SOMETIMES,
                NEVER
        });
    }

    private final int nodeCount = 3;

    @Test
    public void testForceStart_onMissingNode() {
        final AtomicBoolean crashNode = new AtomicBoolean();
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new FailOnPrepareComplete(crashNode, forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount - 1, forceStartFlag);
    }

    @Test
    public void testForceStart_afterJoin_forceStartMaster() {
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnAllMembersJoin(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount, forceStartFlag);
    }

    @Test
    public void testForceStart_afterPartitionTableValidation() {
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnPartitionTableValidationSuccessful(forceStartFlag, nodeCount);
            }
        };

        testForceStart(supplier, nodeCount, forceStartFlag);
    }

    @Test
    public void testForceStart_duringPartitionTableValidation() {
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnMasterOnPartitionTableReceived(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount, forceStartFlag);
    }

    @Test
    public void testForceStart_onDataLoadResult() {
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnMasterOnDataLoadResult(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount, forceStartFlag);
    }

    private void testForceStart(Supplier<ClusterHotRestartEventListener> listenerSupplier, int expectedNodeCount,
                                final AtomicBoolean forceStartFlag) {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, listenerSupplier.get());
        }

        instances = restartInstances(addresses, listeners);
        assertInstancesJoined(expectedNodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Expected forceStartFlag to become true", forceStartFlag.get());
            }
        });
    }

    @Test
    public void testForceStart_duringDataLoad() throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            MockHotRestartService service = getNode(instance).nodeEngine.getService(MockHotRestartService.NAME);
            service.put();
        }

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new TriggerForceStartDuringDataLoad());
        }

        instances = restartInstances(addresses, listeners);
        assertInstancesJoined(addresses.length, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    // In 3.7, we had a bug as following:
    // After a member makes a graceful restart after a force start, it force starts itself instead of doing a restart.
    // We fixed this bug in 3.8 and verify the correct behaviour with this test.
    @Test
    public void testForceStart_thenGracefulRestart() {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new TriggerForceStartOnAllMembersJoin(forceStartFlag));
        }

        instances = restartInstances(addresses, listeners);
        assertInstancesJoined(addresses.length, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        HazelcastInstance instanceToRestart = null;
        for (HazelcastInstance instance : instances) {
            if (!getNode(instance).isMaster()) {
                instanceToRestart = instance;
                break;
            }
        }

        assertNotNull("instanceToRestart should not be null", instanceToRestart);

        changeClusterStateEventually(instanceToRestart, ClusterState.FROZEN);
        Address address = getAddress(instanceToRestart);
        instanceToRestart.getLifecycleService().terminate();
        restartInstance(address);
    }

    @Test
    public void testForceStart_thenGracefulRestart_dataIsPreserved() {
        final AtomicBoolean forceStartFlag = new AtomicBoolean();
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnAllMembersJoin(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount, forceStartFlag);

        HazelcastInstance instanceToRestart = null;
        for (HazelcastInstance instance : getAllInstances()) {
            if (!getNode(instance).isMaster()) {
                instanceToRestart = instance;
                break;
            }
        }

        assertNotNull("instanceToRestart should not be null", instanceToRestart);

        String key = generateKeyOwnedBy(instanceToRestart);
        String value = randomString();
        instanceToRestart.getMap(mapNames[0]).put(key, value);

        // since partition table is persisted async by now, just make sure it is persisted to disk before termination
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instanceToRestart).getNodeExtension();
        nodeExtension.onPartitionStateChange();

        changeClusterStateEventually(instanceToRestart, ClusterState.FROZEN);
        Address address = getAddress(instanceToRestart);
        instanceToRestart.getLifecycleService().shutdown();
        instanceToRestart = restartInstance(address);

        assertEquals("Expected same value for key owned by instanceToRestart",
                value, instanceToRestart.getMap(mapNames[0]).get(key));
    }

    @Override
    protected Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true)
                .setName(MockHotRestartService.NAME)
                .setClassName(MockHotRestartService.class.getName());

        Config config = super.newConfig(listener, clusterStartPolicy);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

    private static class MockHotRestartService implements ManagedService, RamStoreRegistry {

        private static final String NAME = "_MockHotRestartService_";

        private final CountDownLatch loadStarted = new CountDownLatch(1);

        private volatile NodeEngineImpl nodeEngineImpl;
        private volatile HotRestartIntegrationService hotRestartService;

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            nodeEngineImpl = (NodeEngineImpl) nodeEngine;
            NodeExtension nodeExtension = nodeEngineImpl.getNode().getNodeExtension();
            hotRestartService = (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
            hotRestartService.registerRamStoreRegistry(NAME, this);
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        public void put() {
            final String name = "name";
            hotRestartService.ensureHasConfiguration(NAME, name, null);

            int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
            final CountDownLatch latch = new CountDownLatch(partitionCount);
            OperationService operationService = nodeEngineImpl.getOperationService();

            for (int i = 0; i < partitionCount; i++) {
                final int partitionId = i;
                final byte[] bytes = "value".getBytes();
                operationService.execute(new PartitionSpecificRunnable() {
                    @Override
                    public void run() {
                        RamStoreRegistry registry = MockHotRestartService.this;
                        long prefix = hotRestartService.registerRamStore(registry, NAME, name, partitionId);
                        HotRestartStore store = hotRestartService.getOnHeapHotRestartStoreForPartition(partitionId);
                        store.put(new KeyOnHeap(prefix, bytes), bytes, false);
                        latch.countDown();
                    }

                    @Override
                    public int getPartitionId() {
                        return partitionId;
                    }
                });
            }
            assertOpenEventually(latch);
        }

        void awaitLoadStart() {
            assertOpenEventually(loadStarted);
        }

        @Override
        public RamStore ramStoreForPrefix(long prefix) {
            return new MockRamStore(loadStarted, hotRestartService.getClusterMetadataManager(), prefix);
        }

        @Override
        public RamStore restartingRamStoreForPrefix(long prefix) {
            return ramStoreForPrefix(prefix);
        }

        @Override
        public int prefixToThreadId(long prefix) {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockRamStore implements RamStore {
        private final CountDownLatch loadStarted;
        private final ClusterMetadataManager clusterMetadataManager;
        private final long prefix;

        private MockRamStore(CountDownLatch loadStarted, ClusterMetadataManager clusterMetadataManager, long prefix) {
            this.loadStarted = loadStarted;
            this.clusterMetadataManager = clusterMetadataManager;
            this.prefix = prefix;
        }

        @Override
        public KeyHandle toKeyHandle(byte[] key) {
            loadStarted.countDown();

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertNotEquals(CLUSTER_START_IN_PROGRESS, clusterMetadataManager.getHotRestartStatus());
                }
            });

            return new KeyOnHeap(prefix, key);
        }

        @Override
        public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink bufs) throws HotRestartException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void accept(KeyHandle kh, byte[] value) {
        }

        @Override
        public void removeNullEntries(SetOfKeyHandle keyHandles) {
        }
    }

    private class TriggerForceStartOnAllMembersJoin extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean forceStartFlag;

        private volatile Node node;

        TriggerForceStartOnAllMembersJoin(AtomicBoolean forceStartFlag) {
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void afterExpectedMembersJoin(Collection<? extends Member> members) {
            if (forceStartFlag.compareAndSet(false, true)) {
                node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }

    private class TriggerForceStartOnMasterOnPartitionTableReceived extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean forceStartFlag;

        private volatile Node node;

        TriggerForceStartOnMasterOnPartitionTableReceived(AtomicBoolean forceStartFlag) {
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onPartitionTableValidationResult(Member sender, boolean success) {
            if (success) {
                if (forceStartFlag.compareAndSet(false, true)) {
                    node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                }
            } else {
                System.out.println("Partition table validation failed. this node: " + node.getThisAddress()
                        + " sender: " + sender);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }

    private class TriggerForceStartOnMasterOnDataLoadResult extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean forceStartFlag;

        private volatile Node node;

        TriggerForceStartOnMasterOnDataLoadResult(AtomicBoolean forceStartFlag) {
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onHotRestartDataLoadResult(Member sender, boolean success) {
            if (success) {
                if (forceStartFlag.compareAndSet(false, true)) {
                    node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                }
            } else {
                System.out.println("Data load result failed. this node: " + node.getThisAddress() + " sender: " + sender);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }

    // MUST BE USED WITH MASTER
    private static class TriggerForceStartOnPartitionTableValidationSuccessful
            extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final Set<Address> addresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

        private final AtomicBoolean forceStart;
        private final int expectedNodeCount;

        private volatile Node node;

        TriggerForceStartOnPartitionTableValidationSuccessful(AtomicBoolean forceStart, final int expectedNodeCount) {
            this.forceStart = forceStart;
            this.expectedNodeCount = expectedNodeCount;
        }

        @Override
        public void onDataLoadStart(Member member) {
            if (!node.isMaster()) {
                return;
            }
            assertOpenEventually(latch);
        }

        @Override
        public void onPartitionTableValidationResult(Member sender, boolean success) {
            if (!node.isMaster()) {
                return;
            }
            if (success) {
                addresses.add(sender.getAddress());
                if (addresses.size() == expectedNodeCount && forceStart.compareAndSet(false, true)) {
                    node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                    latch.countDown();
                }
            } else {
                System.err.println("Invalid ptable from " + sender);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }

    private class FailOnPrepareComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean crashNode;
        private final AtomicBoolean forceStart;

        private volatile Node node;

        FailOnPrepareComplete(AtomicBoolean crashNode, AtomicBoolean forceStart) {
            this.crashNode = crashNode;
            this.forceStart = forceStart;
        }

        @Override
        public void onPrepareComplete(Collection<? extends Member> members, PartitionTableView partitionTable,
                                      boolean startWithHotRestart) {
            if (crashNode.compareAndSet(false, true)) {
                System.out.println("Crashing " + node.getThisAddress());
                throw new ExpectedRuntimeException();
            }
        }

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            if (currentMembers.size() == nodeCount - 1 && forceStart.compareAndSet(false, true)) {
                node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }

    private class TriggerForceStartDuringDataLoad extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private volatile Node node;

        @Override
        public void onDataLoadStart(final Member member) {
            spawn(new Runnable() {
                @Override
                public void run() {
                    MockHotRestartService service = node.getNodeEngine().getService(MockHotRestartService.NAME);
                    service.awaitLoadStart();
                    node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                }
            });
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }
    }
}
