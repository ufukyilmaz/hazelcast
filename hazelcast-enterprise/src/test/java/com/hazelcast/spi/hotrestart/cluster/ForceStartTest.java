
package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Supplier;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.instance.TestUtil.warmUpPartitions;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ForceStartTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[] {NONE, PARTIAL, ALL});
    }

    private final int nodeCount = 3;

    @Test
    public void testForceStart_onMissingNode() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new FailOnPrepareComplete(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount - 1);
    }

    @Test
    public void testForceStart_afterJoin_forceStartMaster() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnAllMembersJoin(true, forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_afterJoin_forceStartMember() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnAllMembersJoin(false, forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidation_onMaster() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnPartitionTableValiditationSuccessful(true, forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidated_onSingleMember() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnPartitionTableValiditationSuccessful(false, forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidated_onMembers() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnPartitionTableValiditationSuccessful(false, null);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_duringPartitionTableValidation() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnMasterOnPartitionTableReceived(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    @Test
    public void testForceStart_onDataLoadResult() throws Exception {
        Supplier<ClusterHotRestartEventListener> supplier = new Supplier<ClusterHotRestartEventListener>() {
            final AtomicBoolean forceStartFlag = new AtomicBoolean();

            @Override
            public ClusterHotRestartEventListener get() {
                return new TriggerForceStartOnMasterOnDataLoadResult(forceStartFlag);
            }
        };

        testForceStart(supplier, nodeCount);
    }

    private void testForceStart(Supplier<ClusterHotRestartEventListener> listenerSupplier, int expectedNodeCount) throws Exception {
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

        final ClusterHotRestartEventListener listener = new TriggerForceStartDuringDataLoad();
        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, listener);
        }

        instances = restartInstances(addresses, listeners);
        assertInstancesJoined(addresses.length, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testForceStart_thenGracefulRestart() throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new TriggerForceStartOnAllMembersJoin(true, forceStartFlag));
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

        Address address = getAddress(instanceToRestart);
        instanceToRestart.getLifecycleService().terminate();
        restartInstance(address);
    }

    @Override
    protected Config newConfig(String instanceName, ClusterHotRestartEventListener listener) {
        final Config config = super.newConfig(instanceName, listener);
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
        private volatile HotRestartService hotRestartService;
        private final CountDownLatch loadStarted = new CountDownLatch(1);

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            nodeEngineImpl = (NodeEngineImpl) nodeEngine;
            EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) nodeEngineImpl.getNode().getNodeExtension();
            hotRestartService = nodeExtension.getHotRestartService();
            hotRestartService.registerRamStoreRegistry(NAME, this);
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        public void put() throws InterruptedException {
            final String name = "name";
            hotRestartService.ensureHasConfiguration(NAME, name, null);

            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngineImpl.getOperationService();
            final OperationExecutor operationExecutor = operationService.getOperationExecutor();
            final CountDownLatch latch = new CountDownLatch(operationExecutor.getPartitionThreadCount());

            operationExecutor.executeOnPartitionThreads(new Runnable() {
                @Override
                public void run() {
                    try {
                        PartitionOperationThread thread = (PartitionOperationThread) Thread.currentThread();
                        RamStoreRegistry registry = MockHotRestartService.this;
                        long prefix = hotRestartService.registerRamStore(registry, NAME, name, thread.getThreadId());
                        HotRestartStore store = hotRestartService.getOnHeapHotRestartStoreForCurrentThread();
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
            while (clusterMetadataManager.getHotRestartStatus() != FORCE_STARTED) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
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

        private final boolean forceStartMaster;

        private final AtomicBoolean forceStartFlag;

        private Node node;

        TriggerForceStartOnAllMembersJoin(boolean forceStartMaster, AtomicBoolean forceStartFlag) {
            this.forceStartMaster = forceStartMaster;
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onAllMembersJoin(Collection<? extends  Member> members) {

            if (forceStartMaster == node.isMaster() && forceStartFlag.compareAndSet(false, true)) {
                NodeExtension extension = node.getNodeExtension();
                extension.triggerForceStart();
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
                if (node.isMaster() && forceStartFlag.compareAndSet(false, true)) {
                    NodeExtension extension = node.getNodeExtension();
                    extension.triggerForceStart();
                }
            } else {
                System.out.println("Partition table validation failed. this node: " + node.getThisAddress() + " sender: " + sender);
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
                if (node.isMaster() && forceStartFlag.compareAndSet(false, true)) {
                    NodeExtension extension = node.getNodeExtension();
                    extension.triggerForceStart();
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

    private class TriggerForceStartOnPartitionTableValiditationSuccessful
            extends ClusterHotRestartEventListener implements HazelcastInstanceAware {


        private boolean forceStartMaster;

        private AtomicBoolean forceStartFlag;

        private Node node;

        TriggerForceStartOnPartitionTableValiditationSuccessful(boolean forceStartMaster, AtomicBoolean forceStartFlag) {
            this.forceStartMaster = forceStartMaster;
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onPartitionTableValidationComplete(HotRestartClusterInitializationStatus result) {
            if (result == PARTITION_TABLE_VERIFIED) {
                if (forceStartMaster == node.isMaster() && (forceStartFlag == null || forceStartFlag.compareAndSet(false, true))) {
                    NodeExtension extension = node.getNodeExtension();
                    extension.triggerForceStart();
                }
            } else {
                System.out.println("Partition table validation failed. this node: " + node.getThisAddress() + " result: " + result);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }

    }

    private class FailOnPrepareComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean crashNode;

        private Node node;

        FailOnPrepareComplete(AtomicBoolean crashNode) {
            this.crashNode = crashNode;
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
            if (currentMembers.size() == nodeCount - 1) {
                NodeExtension extension = node.getNodeExtension();
                extension.triggerForceStart();
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
                    NodeExtension extension = node.getNodeExtension();
                    extension.triggerForceStart();
                }
            });
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.node = getNode(instance);
        }

    }
}
