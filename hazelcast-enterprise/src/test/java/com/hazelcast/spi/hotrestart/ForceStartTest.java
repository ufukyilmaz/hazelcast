
package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.instance.TestUtil.warmUpPartitions;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class ForceStartTest extends AbstractHotRestartClusterStartTest {

    private final MockHotRestartService service = new MockHotRestartService();

    private final int nodeCount = 3;

    private final List<Integer> ports = acquirePorts(nodeCount);

    @Test
    public void testForceStart_onMissingNode() throws Exception {
        final AtomicBoolean crashNode = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new FailOnPrepareComplete(crashNode));
        }

        testForceStart(listeners, nodeCount - 1);
    }

    @Test
    public void testForceStart_afterJoin_forceStartMaster() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnAllMembersJoin(true, forceStartFlag));
        }

        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_afterJoin_forceStartMember() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnAllMembersJoin(false, forceStartFlag));
        }

        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidation_onMaster() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnPartitionTableValiditationSuccessful(true, forceStartFlag));
        }
        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidated_onSingleMember() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnPartitionTableValiditationSuccessful(false, forceStartFlag));
        }
        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_afterPartitionTableValidated_onMembers() throws Exception {
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnPartitionTableValiditationSuccessful(false, null));
        }
        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_duringPartitionTableValidation() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnMasterOnPartitionTableReceived(forceStartFlag));
        }
        testForceStart(listeners, nodeCount);
    }

    @Test
    public void testForceStart_onDataLoadResult() throws Exception {
        AtomicBoolean forceStartFlag = new AtomicBoolean();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, new TriggerForceStartOnMasterOnDataLoadResult(forceStartFlag));
        }
        testForceStart(listeners, nodeCount);
    }

    private void testForceStart(Map<Integer, ClusterHotRestartEventListener> listeners, int expectedNodeCount) throws Exception {
        HazelcastInstance[] instances = startInstances(ports);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);
        terminateInstances();

        instances = startInstances(ports, listeners);
        assertInstancesJoined(expectedNodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void testForceStart_duringDataLoad() throws Exception {
        List<Integer> ports = acquirePorts(nodeCount);
        HazelcastInstance[] instances = startInstances(ports);
        warmUpPartitions(instances);
        service.put();
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        terminateInstances();

        final ClusterHotRestartEventListener listener = new TriggerForceStartDuringDataLoad();
        Map<Integer, ClusterHotRestartEventListener> listeners = new HashMap<Integer, ClusterHotRestartEventListener>();
        for (Integer port : ports) {
            listeners.put(port, listener);
        }

        instances = startInstances(ports, listeners);
        assertInstancesJoined(ports.size(), instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Override
    protected Config newConfig(ClusterHotRestartEventListener listener) {
        final Config config = super.newConfig(listener);
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true)
                        .setName(MockHotRestartService.NAME)
                        .setServiceImpl(service)
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
            final CountDownLatch latch = new CountDownLatch(
                    operationExecutor.getPartitionOperationThreadCount());

            operationExecutor.runOnAllPartitionThreads(new Runnable() {
                @Override
                public void run() {
                    try {
                        PartitionOperationThread thread = (PartitionOperationThread) Thread.currentThread();
                        RamStoreRegistry registry = MockHotRestartService.this;
                        long prefix = hotRestartService.registerRamStore(registry, NAME, name, thread.getThreadId());
                        HotRestartStore store = hotRestartService.getOnHeapHotRestartStoreForCurrentThread();
                        byte[] bytes = "value".getBytes();
                        store.put(new KeyOnHeap(prefix, bytes), bytes);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            latch.await();
        }

        public void awaitLoadStart() {
            try {
                loadStarted.await();
            } catch (InterruptedException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        @Override
        public boolean copyEntry(KeyHandle key, int expectedSize, RecordDataSink bufs) throws HotRestartException {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyHandle toKeyHandle(byte[] key) {
            loadStarted.countDown();
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new KeyOnHeap(1, key);
        }

        @Override
        public void accept(KeyHandle hrKey, byte[] value) {
            throw new UnsupportedOperationException();
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
    }

    private class TriggerForceStartOnAllMembersJoin extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final boolean forceStartMaster;

        private final AtomicBoolean forceStartFlag;

        private Node node;

        public TriggerForceStartOnAllMembersJoin(boolean forceStartMaster, AtomicBoolean forceStartFlag) {
            this.forceStartMaster = forceStartMaster;
            this.forceStartFlag = forceStartFlag;
        }

        @Override
        public void onAllMembersJoin(Collection<Address> members) {

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

        public TriggerForceStartOnMasterOnPartitionTableReceived(AtomicBoolean forceStartFlag) {
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

        public TriggerForceStartOnMasterOnDataLoadResult(AtomicBoolean forceStartFlag) {
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

        public TriggerForceStartOnPartitionTableValiditationSuccessful(boolean forceStartMaster, AtomicBoolean forceStartFlag) {
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

        public FailOnPrepareComplete(AtomicBoolean crashNode) {
            this.crashNode = crashNode;
        }

        @Override
        public void onPrepareComplete(Collection<Address> members, Address[][] partitionTable,
                                      boolean startWithHotRestart) {
            if (crashNode.compareAndSet(false, true)) {
                System.out.println("Crashing " + node.getThisAddress());
                throw new ExpectedRuntimeException();
            }
        }

        @Override
        public void beforeAllMembersJoin(Collection<Member> currentMembers) {
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
