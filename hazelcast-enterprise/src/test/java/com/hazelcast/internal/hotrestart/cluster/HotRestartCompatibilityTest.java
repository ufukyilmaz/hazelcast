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
import com.hazelcast.internal.util.UuidUtil;
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

import java.io.File;
import java.util.Random;
import java.util.concurrent.Callable;
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
        final HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount()];
        final Config[] configs = new Config[instances.length];

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            configs[i] = createConfig(5701 + i);
            instances[i] = factory.newHazelcastInstance(configs[i]);
        }
        assertClusterSizeEventually(instances.length, instances);

        // initialize partition table
        warmUpPartitions(instances);
        int partitionStateVersion = getPartitionService(instances[0]).getPartitionStateVersion();

        // shutdown cluster with previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with latest members but with previous cluster version
        Future<HazelcastInstance>[] futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            final int ix = i;
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return Hazelcast.newHazelcastInstance(configs[ix]);
                }
            });
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        int partitionStateVersionNew0 = getPartitionService(instances[0]).getPartitionStateVersion();
        for (HazelcastInstance instance : instances) {
            int partitionStateVersionNew = getPartitionService(instance).getPartitionStateVersion();
            assertEquals(partitionStateVersionNew0, partitionStateVersion);
            assertThat(partitionStateVersionNew, greaterThanOrEqualTo(partitionStateVersion));
        }
        partitionStateVersion = partitionStateVersionNew0;

        // shutdown cluster with the previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with previous cluster and member versions
        factory = new CompatibilityTestHazelcastInstanceFactory();
        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            final int ix = i;
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return Hazelcast.newHazelcastInstance(configs[ix]);
                }
            });
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        partitionStateVersionNew0 = getPartitionService(instances[0]).getPartitionStateVersion();
        for (HazelcastInstance instance : instances) {
            int partitionStateVersionNew = getPartitionService(instance).getPartitionStateVersion();
            assertEquals(partitionStateVersionNew0, partitionStateVersion);
            assertThat(partitionStateVersionNew, greaterThanOrEqualTo(partitionStateVersion));
        }
    }

    @Test
    public void fullStart_withUpgrade() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount() + 1];
        final Config[] configs = new Config[instances.length];

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            configs[i] = createConfig(5701 + i);
            instances[i] = factory.newHazelcastInstance(configs[i]);
        }
        assertClusterSizeEventually(instances.length, instances);

        // initialize partition table
        warmUpPartitions(instances);
        int partitionStateVersion = getPartitionService(instances[0]).getPartitionStateVersion();

        // shutdown cluster with previous cluster version
        instances[0].getCluster().shutdown();

        // restart cluster with latest members but with previous cluster version
        Future<HazelcastInstance>[] futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            final int ix = i;
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return Hazelcast.newHazelcastInstance(configs[ix]);
                }
            });
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        PartitionTableView partitionTableView0 = getPartitionService(instances[0]).createPartitionTableView();
        for (HazelcastInstance instance : instances) {
            PartitionTableView partitionTableView = getPartitionService(instance).createPartitionTableView();
            assertThat(partitionTableView.getVersion(), greaterThanOrEqualTo(partitionStateVersion));
            assertEquals(partitionTableView0, partitionTableView);
        }
        partitionStateVersion = partitionTableView0.getVersion();

        // upgrade cluster to the latest version
        instances[0].getCluster().changeClusterVersion(Version.of(CURRENT_VERSION));

        // shutdown cluster with the latest cluster version
        instances[0].getCluster().shutdown();

        // shuffle configs, since hot-restart is not address dependent anymore
        shuffle(configs);

        // restart cluster with latest cluster version
        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            final int ix = i;
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return Hazelcast.newHazelcastInstance(configs[ix]);
                }
            });
        }
        for (int i = 0; i < instances.length; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances.length, instances);

        // check partition tables
        partitionTableView0 = getPartitionService(instances[0]).createPartitionTableView();
        for (HazelcastInstance instance : instances) {
            PartitionTableView partitionTableView = getPartitionService(instance).createPartitionTableView();
            assertThat(partitionTableView.getVersion(), greaterThanOrEqualTo(partitionStateVersion));
            assertEquals(partitionTableView0, partitionTableView);
        }
    }

    @Test
    public void rollingRestart() {
        final HazelcastInstance[] instances = new HazelcastInstance[getKnownPreviousVersionsCount() + 1];
        final Config[] configs = new Config[instances.length];

        // prepare configurations and start instances
        for (int i = 0; i < instances.length; i++) {
            configs[i] = createConfig(5701 + i);
            instances[i] = factory.newHazelcastInstance(configs[i]);
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
            instances[i] = Hazelcast.newHazelcastInstance(configs[i]);
            assertClusterSizeEventually(instances.length, instances);

            // check partition tables
            PartitionTableView partitionTableView = getPartitionService(instances[i]).createPartitionTableView();
            assertEquals(partitionTableView0, partitionTableView);
        }

        // upgrade cluster to the latest version
        instances[0].getCluster().changeClusterVersion(Version.of(CURRENT_VERSION));
    }

    private Config createConfig(int port) {
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j2");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(port);
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true).clear();

        for (int i = 0; i < getKnownPreviousVersionsCount() + 1; i++) {
            tcpIpConfig.addMember("127.0.0.1:" + (5701 + i));
        }

        HotRestartPersistenceConfig hotRestartConfig = config.getHotRestartPersistenceConfig();
        hotRestartConfig.setEnabled(true)
                // For 3.11, we need to define a unique non-shared hot-restart directory
                .setBaseDir(new File(hotRestartFolderRule.getBaseDir(), UuidUtil.newUnsecureUuidString()))
                .setValidationTimeoutSeconds(30).setDataLoadTimeoutSeconds(30);

        return config;
    }

    private static void shuffle(Object[] array) {
        int index;
        Object temp;
        Random random = new Random();
        for (int i = array.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }
}
