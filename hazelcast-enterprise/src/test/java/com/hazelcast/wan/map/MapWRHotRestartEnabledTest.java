package com.hazelcast.wan.map;

import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.util.Collection;

import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.createFolder;
import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static java.util.Arrays.asList;

/**
 * WAN replication tests for hot-restart enabled maps.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapWRHotRestartEnabledTest extends MapWanReplicationTestSupport {

    @Rule
    public TestName testName = new TestName();

    private File folder;

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

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
    @Ignore
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        configA.getMapConfig("default")
               .getHotRestartConfig()
               .setEnabled(true)
               .setFsync(false);

        if (consistencyCheckStrategy == MERKLE_TREES) {
            configA.getMapMerkleTreeConfig("default")
                   .setEnabled(true)
                   .setDepth(5);
            configB.getMapMerkleTreeConfig("default")
                   .setEnabled(true)
                   .setDepth(5);
        }

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
