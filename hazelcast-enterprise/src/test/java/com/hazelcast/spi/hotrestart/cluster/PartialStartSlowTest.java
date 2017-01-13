package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.util.Preconditions.checkFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(SlowTest.class)
public class PartialStartSlowTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{NONE, PARTIAL, ALL});
    }

    private final int nodeCount = 5;

    @Before
    public void init() {
        // tests in this class don't depend on validation timeout
        validationTimeoutInSeconds = Integer.MAX_VALUE;
        dataLoadTimeoutInSeconds = 30;
    }

    @Test
    public void timeout_onMissingLoadStatus_PARTIAL_RECOVERY_MOST_RECENT_whenClusterState_PASSIVE() {
        testTimeoutOnMissingLoadStatus(PARTIAL_RECOVERY_MOST_RECENT);
    }

    @Test
    public void timeout_onMissingLoadStatus_PARTIAL_RECOVERY_MOST_COMPLETE_whenClusterState_PASSIVE() {
        testTimeoutOnMissingLoadStatus(PARTIAL_RECOVERY_MOST_COMPLETE);
    }

    private void testTimeoutOnMissingLoadStatus(HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        checkFalse(clusterStartPolicy == HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY,
                "invalid cluster start policy for partial start test");

        HazelcastInstance[] instances = startInstancesAndChangeClusterState(ClusterState.PASSIVE);
        Address[] addresses = getAddresses(instances);
        terminateInstances();

        AtomicReference<String> excludedUuid = new AtomicReference<String>();
        Map<Address, ClusterHotRestartEventListener> listeners = new HashMap<Address, ClusterHotRestartEventListener>();
        for (Address address : addresses) {
            listeners.put(address, new BlockOnLoadStart(excludedUuid));
        }

        instances = restartInstances(addresses, listeners, clusterStartPolicy);
        assertInstancesJoined(nodeCount - 1, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        assertNotNull(excludedUuid.get());
        for (HazelcastInstance instance : instances) {
            assertNotEquals(excludedUuid.get(), getNode(instance).getThisUuid());
        }
    }

    private HazelcastInstance[] startInstancesAndChangeClusterState(ClusterState clusterState) {
        HazelcastInstance[] instances = startNewInstances(nodeCount);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], clusterState);
        return instances;
    }

    private class BlockOnLoadStart extends ClusterHotRestartEventListener implements HazelcastInstanceAware {

        private final AtomicReference<String> excludedUuid;

        private Node node;

        BlockOnLoadStart(AtomicReference<String> excludedUuid) {
            this.excludedUuid = excludedUuid;
        }

        @Override
        public void onDataLoadStart(Address address) {
            if (!node.isMaster() && excludedUuid.compareAndSet(null, node.getThisUuid())) {
                final HotRestartIntegrationService hotRestartService =
                        (HotRestartIntegrationService) node.getNodeExtension().getInternalHotRestartService();
                ClusterMetadataManager clusterMetadataManager = hotRestartService.getClusterMetadataManager();
                while (clusterMetadataManager.getHotRestartStatus() == HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS) {
                    sleepAtLeastMillis(100);
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            node = getNode(instance);
        }
    }
}
