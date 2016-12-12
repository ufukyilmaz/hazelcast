package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;


/**
 * WAN replication tests for hot-restart enabled maps
 */
@Category(SlowTest.class)
public class MapWRHotRestartEnabledTest extends MapWanReplicationTestSupport {

    private File folder;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() {
        super.setup();
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
        configA.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(folder);
    }

    @After
    public void cleanup() {
        super.cleanup();
        if (folder != null) {
            delete(folder);
        }
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
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotIn(clusterB, "map", 0, 1000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncMap("atob", "B", "map");

        assertKeysIn(clusterB, "map", 0, 1000);
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
            clusterA[i] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        }
    }
}
