package com.hazelcast.internal.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartSplitBrainTest extends SplitBrainTestSupport {

    private static final String HR_MAP_PREFIX = "hr-map-*";

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    private File baseDir;
    private String mapName1 = HR_MAP_PREFIX + 1;
    private String mapName2 = HR_MAP_PREFIX + 2;

    @Override
    protected void onBeforeSetup() {
        super.onBeforeSetup();
        baseDir = hotRestartFolderRule.getBaseDir();
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(baseDir);
        config.getMapConfig(HR_MAP_PREFIX)
                .getHotRestartConfig().setEnabled(true);
        return config;
    }

    @Override
    protected int[] brains() {
        return new int[]{3, 2};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);

        IMap<Object, Object> map1 = instances[0].getMap(mapName1);
        fillMap(map1);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        assertPartitionStateVersions(instances);
        assertClusterStates(instances);

        IMap<Object, Object> map2 = instances[0].getMap(mapName2);
        fillMap(map2);

        Address[] addresses = getAddresses(instances);
        shutdownCluster(instances);

        instances = restartCluster(addresses);
        assertClusterSizeEventually(instances.length, instances);

        assertPartitionStateVersions(instances);
        assertClusterStates(instances);

        assertMapContents(instances[0]);
    }

    private void assertMapContents(HazelcastInstance instance) {
        IMap<Object, Object> map1 = instance.getMap(mapName1);
        IMap<Object, Object> map2 = instance.getMap(mapName2);

        for (int i = 0; i < 1000; i++) {
            assertEquals(i, map1.get(i));
            assertEquals(i, map2.get(i));
        }
    }

    private void fillMap(IMap<Object, Object> map) {
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }
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
                    return factory.newHazelcastInstance(address, config());
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
