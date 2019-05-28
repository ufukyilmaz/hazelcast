package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TriggerForceAndPartialStartTest extends AbstractHotRestartClusterStartTest {

    @Test
    public void test_triggerForceStart_onMaster() {
        test_triggerForceStart(false, true);
    }

    @Test
    public void test_triggerForceStart_onNonMaster() {
        test_triggerForceStart(false, false);
    }

    @Test
    public void test_triggerPartialStart_onMaster() {
        test_triggerForceStart(true, true);
    }

    @Test
    public void test_triggerPartialStart_onNonMaster() {
        test_triggerForceStart(true, false);
    }

    private void test_triggerForceStart(boolean partialStart, boolean onMaster) {
        int numberOfInstances = 3;
        HazelcastInstance[] instances = startNewInstances(numberOfInstances);
        assertInstancesJoined(numberOfInstances, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        warmUpPartitions(instances);

        Address[] addresses = Arrays.copyOf(getAddresses(instances), numberOfInstances - 1);
        shutdownCluster(instances);

        int expectedMemberCount = addresses.length;
        instances = restartInstances(addresses, createListenerMap(addresses, partialStart, onMaster, expectedMemberCount),
                HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE);
        assertInstancesJoined(expectedMemberCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        for (HazelcastInstance instance : instances) {
            checkStartResult(partialStart, instance);
        }
    }

    private void checkStartResult(boolean partialStart, HazelcastInstance instance) {
        InternalPartitionService partitionService = getNodeEngineImpl(instance).getPartitionService();
        if (partialStart) {
            assertThat("Expected partition state version to be > 0", partitionService.getPartitionStateVersion(), greaterThan(0));
        } else {
            assertEquals("Expected partition state version to be 0", 0, partitionService.getPartitionStateVersion());
        }
    }

    @Override
    protected Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Config config = super.newConfig(listener, clusterStartPolicy);
        config.getHotRestartPersistenceConfig()
                // tests don't depend on validation timeout, we explicitly trigger force & partial start
                .setValidationTimeoutSeconds(Integer.MAX_VALUE);
        return config;
    }

    private static void shutdownCluster(HazelcastInstance... instances) {
        assertThat("Expected instances to be not empty", instances, not(emptyArray()));
        waitAllForSafeState(instances);
        instances[0].getCluster().shutdown();
    }

    private static Map<Address, ClusterHotRestartEventListener> createListenerMap(Address[] addresses, boolean partialStart,
                                                                                  boolean onMaster, int expectedMemberCount) {
        Map<Address, ClusterHotRestartEventListener> listenerMap =
                new HashMap<Address, ClusterHotRestartEventListener>(addresses.length);
        for (Address address : addresses) {
            listenerMap.put(address, new TriggerForceStart(partialStart, onMaster, expectedMemberCount));
        }
        return listenerMap;
    }

    private static class TriggerForceStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicBoolean flag = new AtomicBoolean(false);

        private final boolean partialStart;
        private final boolean onMaster;
        private final int expectedMemberCount;

        private volatile Node node;

        TriggerForceStart(boolean partialStart, boolean onMaster, int expectedMemberCount) {
            this.partialStart = partialStart;
            this.onMaster = onMaster;
            this.expectedMemberCount = expectedMemberCount;
        }

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            if (currentMembers.size() < expectedMemberCount) {
                return;
            }
            if ((onMaster && !node.isMaster()) || (!onMaster && node.isMaster())) {
                return;
            }
            if (!flag.compareAndSet(false, true)) {
                return;
            }

            InternalHotRestartService service = node.getNodeExtension().getInternalHotRestartService();
            if (partialStart) {
                service.triggerPartialStart();
            } else {
                service.triggerForceStart();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.node = getNode(hazelcastInstance);
        }
    }
}
