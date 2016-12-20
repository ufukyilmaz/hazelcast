package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyForPartition;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getPartitionService;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static com.hazelcast.util.Preconditions.checkFalse;
import static java.lang.System.arraycopy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartialStartTest extends AbstractHotRestartClusterStartTest {

    protected static final Map<Address, ClusterHotRestartEventListener> NO_LISTENERS = Collections.emptyMap();

    @Parameterized.Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{NONE, PARTIAL, ALL});
    }


    private final int nodeCount = 5;

    @Before
    public void init() {
        dataLoadTimeoutInSeconds = 60;
    }

    @Test
    public void trigger_onMissingLoadStatus_PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_PASSIVE()
            throws Exception {
        testTriggerOnMissingLoadStatus(PARTIAL_RECOVERY_MOST_RECENT);
    }

    @Test
    public void trigger_onMissingLoadStatus_PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterState_PASSIVE()
            throws Exception {
        testTriggerOnMissingLoadStatus(PARTIAL_RECOVERY_MOST_COMPLETE);
    }

    private void testTriggerOnMissingLoadStatus(HotRestartClusterDataRecoveryPolicy clusterStartPolicy)
            throws Exception {
        checkFalse(clusterStartPolicy == HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY,
                "invalid cluster start policy for partial start test");

        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        String excludedUuid = null;
        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(addresses[0])) {
                excludedUuid = getNode(instance).getThisUuid();
                break;
            }
        }

        assertNotNull(excludedUuid);

        terminateInstances();

        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new TriggerPartialStart(nodeCount - 1));
        }

        Address[] newAddresses = new Address[nodeCount - 1];
        arraycopy(addresses, 1, newAddresses, 0, newAddresses.length);
        instances = restartInstances(newAddresses, listeners, clusterStartPolicy);
        assertInstancesJoined(nodeCount - 1, instances, NodeState.PASSIVE, ClusterState.PASSIVE);

        for (HazelcastInstance instance : instances) {
            final InternalHotRestartService hotRestartService =
                    getNode(instance).getNodeExtension().getInternalHotRestartService();
            assertTrue(hotRestartService.getExcludedMemberUuids().contains(excludedUuid));
        }
    }

    @Test
    public void timeout_onMissingMember_PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_PASSIVE()
            throws Exception {
        testTimeoutOnMissingMember(PARTIAL_RECOVERY_MOST_RECENT);
    }

    @Test
    public void timeout_onMissingMember_PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterState_PASSIVE()
            throws Exception {
        testTimeoutOnMissingMember(PARTIAL_RECOVERY_MOST_COMPLETE);
    }

    private void testTimeoutOnMissingMember(HotRestartClusterDataRecoveryPolicy clusterStartPolicy)
            throws Exception {
        checkFalse(clusterStartPolicy == HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY,
                "invalid cluster start policy for partial start test");

        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        String excludedUuid = null;
        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(addresses[0])) {
                excludedUuid = getNode(instance).getThisUuid();
                break;
            }
        }

        assertNotNull(excludedUuid);

        terminateInstances();

        Address[] newAddresses = new Address[nodeCount - 1];
        arraycopy(addresses, 1, newAddresses, 0, newAddresses.length);
        instances = restartInstances(newAddresses, NO_LISTENERS, clusterStartPolicy);
        assertInstancesJoined(nodeCount - 1, instances, NodeState.PASSIVE, ClusterState.PASSIVE);

        for (HazelcastInstance instance : instances) {
            final InternalHotRestartService hotRestartService =
                    getNode(instance).getNodeExtension().getInternalHotRestartService();
            assertTrue(hotRestartService.getExcludedMemberUuids().contains(excludedUuid));
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterState_PASSIVE()
            throws Exception {
        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        for (int i = 1; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();
        }

        String excludedUuid = getNode(instances[0]).getThisUuid();
        terminateWithOverwrittenIncrementedPartitionTableVersion(instances[0]);

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoined(nodeCount - 1, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        for (HazelcastInstance instance : instances) {
            assertFalse(excludedUuid.equals(getNode(instance).getThisUuid()));
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterState_ACTIVE()
            throws Exception {
        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance[] survivingInstances = new HazelcastInstance[nodeCount - 1];
        arraycopy(instances, 1, survivingInstances, 0, survivingInstances.length);
        for (int i = 1; i < instances.length; i++) {
            terminateWithOverwrittenACTIVEClusterState(instances[i]);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        String excludedUuid = terminateWithOverwrittenACTIVEClusterState(instances[0]);

        partitionLostListener.clear();

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        for (HazelcastInstance instance : instances) {
            assertFalse(excludedUuid.equals(getNode(instance).getThisUuid()));
        }

        waitAllForSafeState(instances);

        List<PartitionLostEvent> events = partitionLostListener.getEvents();
        assertFalse(events.isEmpty());
        for (PartitionLostEvent event : events) {
            assertEquals(event.getLostBackupCount(), 0);
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_PASSIVE()
            throws Exception {
        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        Set<String> excludedUuids = new HashSet<String>();
        for (int i = 1; i < instances.length; i++) {
            excludedUuids.add(getNode(instances[i]).getThisUuid());
            instances[i].getLifecycleService().terminate();
        }

        terminateWithOverwrittenIncrementedPartitionTableVersion(instances[0]);

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_RECENT);
        assertInstancesJoined(1, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        assertFalse(excludedUuids.contains(getNode(instances[0]).getThisUuid()));
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterStateACTIVE_withMapData()
            throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        Address[] addresses = getAddresses(instances);
        warmUpPartitions(instances);

        HazelcastInstance survivingInstances[] = new HazelcastInstance[instances.length - 1];
        arraycopy(instances, 1, survivingInstances, 0, survivingInstances.length);
        int[] survivingInstanceReplicaIndices = getSurvivingReplicaIndices(survivingInstances);

        int partitionCount = getPartitionService(instances[0]).getPartitionCount();
        String[] keys = new String[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            HazelcastInstance instance = instances[0];
            String key = generateKeyForPartition(instance, partitionId);
            keys[partitionId] = key;

            for (String mapName : mapNames) {
                instance.getMap(mapName).put(key, key);
            }
        }

        String excludedUuid = getNode(instances[0]).getThisUuid();
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            terminateWithOverwrittenACTIVEClusterState(instance);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        terminateWithOverwrittenACTIVEClusterState(instances[0]);


        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        for (HazelcastInstance instance : instances) {
            assertNotEquals(excludedUuid, getNode(instance).getThisUuid());
        }
        waitAllForSafeState(instances);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            String key = keys[partitionId];
            final int survivingReplicaIndex = survivingInstanceReplicaIndices[partitionId];
            for (int i = survivingReplicaIndex; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                boolean success = instances[0].getMap(mapNames[i]).containsKey(key);
                assertTrue("key: " + key + " in partitionId=" + partitionId + " map=" + mapNames[i] + " survived replica index=" + survivingReplicaIndex, success);
            }
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_ACTIVE_withMapData()
            throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        Address[] addresses = getAddresses(instances);
        warmUpPartitions(instances);

        int[] survivingInstanceReplicaIndices = getSurvivingReplicaIndices(instances[0]);

        int partitionCount = getPartitionService(instances[0]).getPartitionCount();
        String[] keys = new String[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            HazelcastInstance instance = instances[0];
            String key = generateKeyForPartition(instance, partitionId);
            keys[partitionId] = key;

            for (String mapName : mapNames) {
                instance.getMap(mapName).put(key, key);
            }
        }

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Set<String> excludedUuids = new HashSet<String>();
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            excludedUuids.add(getNode(instance).getThisUuid());
            terminateWithOverwrittenACTIVEClusterState(instance);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        terminateWithOverwrittenACTIVEClusterState(instances[0]);

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_RECENT);

        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        for (HazelcastInstance instance : instances) {
            assertFalse(excludedUuids.contains(getNode(instance).getThisUuid()));
        }
        waitAllForSafeState(instances);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            String key = keys[partitionId];
            final int survivingReplicaIndex = survivingInstanceReplicaIndices[partitionId];
            for (int i = survivingReplicaIndex; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                boolean success = instances[0].getMap(mapNames[i]).containsKey(key);
                assertTrue("key: " + key + " in partitionId=" + partitionId + " map=" + mapNames[i] + " survived replica index=" + survivingReplicaIndex, success);
            }
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_ACTIVE_withCacheData()
            throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        Address[] addresses = getAddresses(instances);
        warmUpPartitions(instances);

        int[] survivingInstanceReplicaIndices = getSurvivingReplicaIndices(instances[0]);

        int partitionCount = getPartitionService(instances[0]).getPartitionCount();
        String[] keys = new String[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            HazelcastInstance instance = instances[0];
            String key = generateKeyForPartition(instance, partitionId);
            keys[partitionId] = key;

            for (String cacheName : cacheNames) {
                instance.getCacheManager().getCache(cacheName).put(key, key);
            }
        }

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Set<String> excludedUuids = new HashSet<String>();
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            excludedUuids.add(getNode(instance).getThisUuid());
            terminateWithOverwrittenACTIVEClusterState(instance);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        terminateWithOverwrittenACTIVEClusterState(instances[0]);

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_RECENT);

        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        for (HazelcastInstance instance : instances) {
            assertFalse(excludedUuids.contains(getNode(instance).getThisUuid()));
        }
        waitAllForSafeState(instances);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++ ) {
            String key = keys[partitionId];
            final int survivingReplicaIndex = survivingInstanceReplicaIndices[partitionId];
            for (int i = survivingReplicaIndex; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                boolean success = instances[0].getCacheManager().getCache(cacheNames[i]).containsKey(key);
                assertTrue("key: " + key + " in partitionId=" + partitionId + " cache=" + cacheNames[i] + " survived replica index=" + survivingReplicaIndex, success);
            }
        }
    }

    @Test
    public void PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_ACTIVE()
            throws Exception {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        Address[] addresses = getAddresses(instances);
        warmUpPartitions(instances);

        int[] survivingInstanceReplicaIndices = getSurvivingReplicaIndices(instances[0]);

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Set<String> excludedUuids = new HashSet<String>();
        for (int i = 1; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            excludedUuids.add(getNode(instance).getThisUuid());
            terminateWithOverwrittenACTIVEClusterState(instance);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        terminateWithOverwrittenACTIVEClusterState(instances[0]);

        partitionLostListener.clear();

        instances = restartInstances(addresses, NO_LISTENERS, PARTIAL_RECOVERY_MOST_RECENT);

        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        for (HazelcastInstance instance : instances) {
            assertFalse(excludedUuids.contains(getNode(instance).getThisUuid()));
        }

        waitAllForSafeState(instances);

        List<PartitionLostEvent> events = partitionLostListener.getEvents();
        assertFalse(events.isEmpty());
        for (PartitionLostEvent event : events) {
            assertTrue(event.getLostBackupCount() < survivingInstanceReplicaIndices[event.getPartitionId()]);
        }
    }

    @Test
    public void missingNodeCanJoinBackOnPartialStart() throws InterruptedException {
        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance[] survivingInstances = new HazelcastInstance[nodeCount - 1];
        arraycopy(instances, 1, survivingInstances, 0, survivingInstances.length);
        for (int i = 1; i < instances.length; i++) {
            terminateWithOverwrittenACTIVEClusterState(instances[i]);
        }

        overwriteIncrementedPartitionTableVersion(instances[0]);
        AtomicReference<String> restartedUuid = new AtomicReference<String>();
        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        listeners.put(getAddress(instances[0]), new RestartOnComplete(restartedUuid));
        String excludedUuid = terminateWithOverwrittenACTIVEClusterState(instances[0]);

        restartInstances(addresses, listeners, PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoinedEventually(nodeCount, NodeState.ACTIVE, ClusterState.ACTIVE);

        assertEquals(excludedUuid, restartedUuid.get());
    }

    private int[] getSurvivingReplicaIndices(HazelcastInstance... instances) {
        InternalPartitionService partitionService = getPartitionService(instances[0]);
        int[] survivingInstanceReplicaIndices = new int[partitionService.getPartitionCount()];
        Arrays.fill(survivingInstanceReplicaIndices, Integer.MAX_VALUE);
        for (HazelcastInstance instance : instances) {
            for (int partitionId = 0; partitionId < survivingInstanceReplicaIndices.length; partitionId++) {
                InternalPartition partition = partitionService.getPartition(partitionId);
                final int i = partition.getReplicaIndex(getAddress(instance));
                if (i < survivingInstanceReplicaIndices[partitionId]) {
                    survivingInstanceReplicaIndices[partitionId] = i;
                }
            }
        }

        return survivingInstanceReplicaIndices;
    }

    private void overwriteIncrementedPartitionTableVersion(HazelcastInstance instance) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        partitionService.getPartitionStateManager().incrementVersion();
        final HotRestartIntegrationService hotRestartService =
                (HotRestartIntegrationService) getNode(instance).getNodeExtension().getInternalHotRestartService();
        hotRestartService.getClusterMetadataManager().onPartitionStateChange();
    }

    private void terminateWithOverwrittenIncrementedPartitionTableVersion(HazelcastInstance instance) {
        overwriteIncrementedPartitionTableVersion(instance);
        instance.getLifecycleService().terminate();
    }

    private String terminateWithOverwrittenACTIVEClusterState(HazelcastInstance instance) {
        String uuid = getNode(instance).getThisUuid();
        final HotRestartIntegrationService hotRestartService =
                (HotRestartIntegrationService) getNode(instance).getNodeExtension().getInternalHotRestartService();
        hotRestartService.getClusterMetadataManager().onClusterStateChange(ClusterState.ACTIVE);
        instance.getLifecycleService().terminate();

        return uuid;
    }

    protected HazelcastInstance[] startInstancesAndChangeClusterState(ClusterState clusterState) throws InterruptedException {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], clusterState);
        return instances;
    }


    private class TriggerPartialStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final int expectedNodeCount;

        private Node node;

        TriggerPartialStart(int expectedNodeCount) {
            this.expectedNodeCount = expectedNodeCount;
        }

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            if (node.isMaster() && currentMembers.size() == expectedNodeCount) {
                node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            node = getNode(instance);
        }

    }

    private class RestartOnComplete extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicReference<String> restartedUuid;

        private Node node;

        RestartOnComplete(AtomicReference<String> restartedUuid) {
            this.restartedUuid = restartedUuid;
        }

        @Override
        public void onHotRestartDataLoadComplete(HotRestartClusterStartStatus result, Set<String> excludedMemberUuids) {
            if (result == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(node.getThisUuid())
                    && restartedUuid.compareAndSet(null, node.getThisUuid())) {
                startNodeAfterTermination(node);
                throw new HotRestartException("hot restart is failed manually!");
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            node = getNode(instance);
        }

    }

}
