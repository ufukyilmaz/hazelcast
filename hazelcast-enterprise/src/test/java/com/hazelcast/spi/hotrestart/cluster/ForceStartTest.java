package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ForceStartTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{NONE, PARTIAL, ALL});
    }

    private final int nodeCount = 3;

    @Before
    public void init() {
        validationTimeoutInSeconds = 30;
        dataLoadTimeoutInSeconds = 30;
    }

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
            public void run() throws Exception {
                assertTrue(forceStartFlag.get());
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

        assertNotNull(instanceToRestart);

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

        assertNotNull(instanceToRestart);

        final String key = generateKeyOwnedBy(instanceToRestart);
        instanceToRestart.getMap(mapNames[0]).put(key, key);

        // since partition table is persisted async by now, just make sure it is persisted to disk before termination
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instanceToRestart).getNodeExtension();
        nodeExtension.onPartitionStateChange();

        changeClusterStateEventually(instanceToRestart, ClusterState.FROZEN);
        Address address = getAddress(instanceToRestart);
        instanceToRestart.getLifecycleService().shutdown();
        instanceToRestart = restartInstance(address);

        assertEquals(key, instanceToRestart.getMap(mapNames[0]).get(key));
    }

    @Override
    protected Config newConfig(String instanceName, ClusterHotRestartEventListener listener,
                               HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        final Config config = super.newConfig(instanceName, listener, clusterStartPolicy);
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true)
                        .setName(MockHotRestartService.NAME)
                        .setClassName(MockHotRestartService.class.getName())
        );
        return config;
    }

    private static class MockHotRestartService implements ManagedService, RamStoreRegistry, RamStore {

        private static final String NAME = "_MockHotRestartService_";

        private volatile NodeEngineImpl nodeEngineImpl;
        private volatile HotRestartIntegrationService hotRestartService;
        private final CountDownLatch loadStarted = new CountDownLatch(1);

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            nodeEngineImpl = (NodeEngineImpl) nodeEngine;
            hotRestartService =
                    (HotRestartIntegrationService) nodeEngineImpl.getNode().getNodeExtension().getInternalHotRestartService();
            hotRestartService.registerRamStoreRegistry(NAME, this);
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        public void put() throws Exception {
            final String name = "name";
            hotRestartService.ensureHasConfiguration(NAME, name, null);

            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngineImpl.getOperationService();
            final OperationExecutor operationExecutor = operationService.getOperationExecutor();
            final CountDownLatch latch = new CountDownLatch(operationExecutor.getPartitionThreadCount());
            final int partitionId = 123;

            operationExecutor.executeOnPartitionThreads(new Runnable() {
                @Override
                public void run() {
                    try {
                        RamStoreRegistry registry = MockHotRestartService.this;
                        long prefix = hotRestartService.registerRamStore(registry, NAME, name, partitionId);
                        HotRestartStore store = hotRestartService.getOnHeapHotRestartStoreForPartition(partitionId);
                        byte[] bytes = "value".getBytes();
                        store.put(new KeyOnHeap(prefix, bytes), bytes, false);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            latch.await();
        }

        void awaitLoadStart() {
            try {
                loadStarted.await();
            } catch (InterruptedException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        @Override
        public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink bufs) throws HotRestartException {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyHandle toKeyHandle(byte[] key) {
            loadStarted.countDown();

            ClusterMetadataManager clusterMetadataManager = hotRestartService.getClusterMetadataManager();
            while (clusterMetadataManager.getHotRestartStatus() == HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }

            return new KeyOnHeap(1, key);
        }

        @Override
        public void accept(KeyHandle kh, byte[] value) {
        }

        @Override
        public void removeNullEntries(SetOfKeyHandle keyHandles) {
        }

        @Override
        public RamStore ramStoreForPrefix(long prefix) {
            return this;
        }

        @Override
        public RamStore restartingRamStoreForPrefix(long prefix) {
            return this;
        }

        @Override
        public int prefixToThreadId(long prefix) {
            return 0;
        }
    }

    private class TriggerForceStartOnAllMembersJoin extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean forceStartFlag;

        private Node node;

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

        private AtomicBoolean forceStartFlag;

        private Node node;

        TriggerForceStartOnMasterOnPartitionTableReceived(AtomicBoolean forceStartFlag) {
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onPartitionTableValidationResult(Address sender, boolean success) {
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

        private AtomicBoolean forceStartFlag;

        private Node node;

        TriggerForceStartOnMasterOnDataLoadResult(AtomicBoolean forceStartFlag) {
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onHotRestartDataLoadResult(Address sender, boolean success) {
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

        private final AtomicBoolean forceStart;

        private final int expectedNodeCount;

        private final Set<Address> addresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

        private HazelcastInstance instance;

        TriggerForceStartOnPartitionTableValidationSuccessful(AtomicBoolean forceStart, final int expectedNodeCount) {
            this.forceStart = forceStart;
            this.expectedNodeCount = expectedNodeCount;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onDataLoadStart(Address address) {
            final Node node = getNode(instance);
            if (!node.isMaster()) {
                return;
            }
            assertOpenEventually(latch);
        }

        @Override
        public void onPartitionTableValidationResult(Address sender, boolean success) {
            final Node node = getNode(instance);
            if (!node.isMaster()) {
                return;
            }

            if (success) {
                addresses.add(sender);
                if (addresses.size() == expectedNodeCount && forceStart.compareAndSet(false, true)) {
                    node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                    latch.countDown();
                }
            } else {
                System.err.println("Invalid ptable from " + sender);
            }
        }
    }

    private class FailOnPrepareComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean crashNode;

        private final AtomicBoolean forceStart;

        private Node node;

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

        private Node node;

        @Override
        public void onDataLoadStart(final Address address) {
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
