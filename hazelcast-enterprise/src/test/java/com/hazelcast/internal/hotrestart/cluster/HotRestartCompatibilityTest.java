package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class HotRestartCompatibilityTest extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

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
    public void fullStart_withoutUpgrade() throws Exception {
        HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount()];
        Config config = createConfig();

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        assertClusterSizeEventually(instances.length, instances);

        // initialize partition table
        warmUpPartitions(instances);
        int initialPartitionStateVersion = getPartitionService(instances[0]).getPartitionStateVersion();

        // shutdown cluster with previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with latest members but with previous cluster version
        Future<HazelcastInstance>[] futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(() -> Hazelcast.newHazelcastInstance(config));
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        int partitionStateVersionNew0 = getPartitionService(instances[0]).getPartitionStateVersion();
        for (HazelcastInstance instance : instances) {
            int partitionStateVersionNew = getPartitionService(instance).getPartitionStateVersion();
            assertEquals(partitionStateVersionNew0, partitionStateVersionNew);
            assertThat(partitionStateVersionNew, greaterThanOrEqualTo(initialPartitionStateVersion));
        }
        initialPartitionStateVersion = partitionStateVersionNew0;

        // shutdown cluster with the previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with previous cluster and member versions
        factory = new CompatibilityTestHazelcastInstanceFactory();
        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(() -> Hazelcast.newHazelcastInstance(config));
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        partitionStateVersionNew0 = getPartitionService(instances[0]).getPartitionStateVersion();
        for (HazelcastInstance instance : instances) {
            int partitionStateVersionNew = getPartitionService(instance).getPartitionStateVersion();
            assertEquals(partitionStateVersionNew0, partitionStateVersionNew);
            assertThat(partitionStateVersionNew, greaterThanOrEqualTo(initialPartitionStateVersion));
        }
    }

    @Test
    public void fullStart_withUpgrade() throws Exception {
        HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount() + 1];
        Config config = createConfig();

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        assertClusterSizeEventually(instances.length, instances);

        // initialize partition table
        warmUpPartitions(instances);
        int initialPartitionStateVersion = getPartitionService(instances[0]).getPartitionStateVersion();

        // shutdown cluster with previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with latest members but with previous cluster version
        Future<HazelcastInstance>[] futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(() -> Hazelcast.newHazelcastInstance(config));
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        PartitionTableView partitionTableView0 = getPartitionService(instances[0]).createPartitionTableView();
        for (HazelcastInstance instance : instances) {
            PartitionTableView partitionTableView = getPartitionService(instance).createPartitionTableView();
            assertThat(partitionTableView.version(), greaterThanOrEqualTo(initialPartitionStateVersion));
            assertEquals(partitionTableView0, partitionTableView);
        }

        // upgrade cluster to the latest version
        instances[0].getCluster().changeClusterVersion(Version.of(CURRENT_VERSION));

        // shutdown cluster with the latest cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with latest cluster version
        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(() -> Hazelcast.newHazelcastInstance(config));
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        partitionTableView0 = getPartitionService(instances[0]).createPartitionTableView();
        for (HazelcastInstance instance : instances) {
            PartitionTableView partitionTableView = getPartitionService(instance).createPartitionTableView();
            assertEquals(partitionTableView0.stamp(), partitionTableView.stamp());
            assertEquals(partitionTableView0, partitionTableView);
        }
    }

    @Test
    public void rollingRestart() {
        HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount() + 1];
        Config config = createConfig();

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        assertClusterSizeEventually(instances.length, instances);

        // initialize partition table
        warmUpPartitions(instances);

        // change cluster state to FROZEN
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        PartitionTableView partitionTableView0 = getPartitionService(instances[getKnownPreviousVersionsCount()]).createPartitionTableView();

        // restart previous versions one by one
        for (int i = 0; i < getKnownPreviousVersionsCount(); i++) {
            instances[i].shutdown();
            assertClusterSizeEventually(getKnownPreviousVersionsCount(), instances[getKnownPreviousVersionsCount()]);
            instances[i] = Hazelcast.newHazelcastInstance(config);
            assertClusterSizeEventually(instances.length, instances);

            // check partition tables
            PartitionTableView partitionTableView = getPartitionService(instances[i]).createPartitionTableView();
            assertEquals(partitionTableView0, partitionTableView);
        }

        // upgrade cluster to the latest version
        instances[0].getCluster().changeClusterVersion(Version.of(CURRENT_VERSION));
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j2");

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true).clear();

        for (int i = 0; i < getKnownPreviousVersionsCount() + 1; i++) {
            tcpIpConfig.addMember("127.0.0.1:" + (networkConfig.getPort() + i));
        }

        HotRestartPersistenceConfig hotRestartConfig = config.getHotRestartPersistenceConfig();
        hotRestartConfig.setEnabled(true)
                .setBaseDir(hotRestartFolderRule.getBaseDir())
                .setValidationTimeoutSeconds(30).setDataLoadTimeoutSeconds(30);

        return config;
    }
}
