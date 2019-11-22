package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.properties.ClusterProperty.TCP_JOIN_PORT_TRY_COUNT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ClusterStateCompatibilityTest extends HazelcastTestSupport {

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void previousVersionMember_changesState() {
        testStateChange(0, false);
    }

    @Test
    public void latestVersionMember_changesState() {
        testStateChange(getKnownPreviousVersionsCount(), false);
    }

    @Test
    public void previousVersionMember_changesState_withPartitionsInitialized() {
        testStateChange(0, true);
    }

    @Test
    public void latestVersionMember_changesState_withPartitionsInitialized() {
        testStateChange(getKnownPreviousVersionsCount(), true);
    }

    private void testStateChange(int initiatorIndex, boolean initializePartitions) {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        if (initializePartitions) {
            warmUpPartitions(instances);
        }

        ClusterState newState = ClusterState.NO_MIGRATION;
        changeClusterStateEventually(instances[initiatorIndex], newState);

        for (HazelcastInstance instance : instances) {
            Cluster cluster = instance.getCluster();
            assertEquals(newState, cluster.getClusterState());
        }
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty(TCP_JOIN_PORT_TRY_COUNT.getName(), String.valueOf(getKnownPreviousVersionsCount()));
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");
        return config;
    }
}
