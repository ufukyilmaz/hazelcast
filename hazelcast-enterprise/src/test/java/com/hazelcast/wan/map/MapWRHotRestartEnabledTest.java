package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.createFolder;
import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.nio.IOUtil.deleteQuietly;

/**
 * WAN replication tests for hot-restart enabled maps.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapWRHotRestartEnabledTest extends MapWanReplicationTestSupport {

    @Rule
    public TestName testName = new TestName();

    private File folder;

    @Before
    @Override
    public void setup() {
        folder = isolatedFolder(getClass(), testName);
        createFolder(folder);
        super.setup();
    }

    @After
    @Override
    public void cleanup() {
        super.cleanup();
        deleteQuietly(folder);
    }

    @Test
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        configA.getMapConfig("default")
               .getHotRestartConfig()
               .setEnabled(true)
               .setFsync(false);
        configA.getHotRestartPersistenceConfig()
               .setEnabled(true);
        startClusterAWithDifferentHotRestartConfigs();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();

        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncMap("atob", "B", "map");

        assertKeysInEventually(clusterB, "map", 0, 1000);
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }

    private void startClusterAWithDifferentHotRestartConfigs() {
        for (int i = 0; i < clusterA.length; i++) {
            configA.setInstanceName(configA.getInstanceName() + i);
            configA.getHotRestartPersistenceConfig().setBaseDir(new File(folder, configA.getInstanceName()));
            clusterA[i] = factory.newHazelcastInstance(configA);
        }
    }
}
