package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.Address;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toFileName;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartSplitBrainTest extends SplitBrainTestSupport {

    @Rule
    public TemporaryFolder hotRestartFolder = new TemporaryFolder();

    private File baseDir;

    @Override
    protected void onBeforeSetup() {
        super.onBeforeSetup();
        try {
            baseDir = hotRestartFolder.newFolder();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Config newConfig(Address address) {
        Config config = super.config();
        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())));
        return config;
    }

    @Override
    protected int[] brains() {
        return new int[]{3, 2};
    }

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[clusterSize];
        factory = createHazelcastInstanceFactory(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            Address address = factory.nextAddress();
            HazelcastInstance hz = factory.newHazelcastInstance(address, newConfig(address));
            hazelcastInstances[i] = hz;
        }
        return hazelcastInstances;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        assertPartitionStateVersions(instances);
        assertClusterStates(instances);

        Address[] addresses = getAddresses(instances);
        shutdownCluster(instances);

        instances = restartCluster(addresses);
        assertClusterSize(instances.length, instances);

        assertPartitionStateVersions(instances);
        assertClusterStates(instances);
    }

    private void assertClusterStates(HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals(ClusterState.ACTIVE, getClusterService(instance).getClusterState());
        }
    }

    private void assertPartitionStateVersions(HazelcastInstance[] instances) {
        int partitionStateVersion = getPartitionService(instances[0]).getPartitionStateVersion();
        for (HazelcastInstance instance : instances) {
            assertEquals(partitionStateVersion, getPartitionService(instance).getPartitionStateVersion());
        }
    }

    private Address[] getAddresses(HazelcastInstance[] instances) {
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = getAddress(instances[i]);
        }
        return addresses;
    }

    private HazelcastInstance[] restartCluster(final Address[] addresses) throws Exception {
        int[] brains = brains();
        int clusterSize = brains[0] + brains[1];
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        Future<HazelcastInstance>[] futures = new Future[clusterSize];

        for (int i = 0; i < clusterSize; i++) {
            final Address address = addresses[i];
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return factory.newHazelcastInstance(address, newConfig(address));
                }
            });
        }

        for (int i = 0; i < clusterSize; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        return instances;
    }

    private void shutdownCluster(HazelcastInstance... instances) {
        assertThat(instances, not(emptyArray()));
        waitAllForSafeState(instances);
        instances[0].getCluster().shutdown();
    }
}
