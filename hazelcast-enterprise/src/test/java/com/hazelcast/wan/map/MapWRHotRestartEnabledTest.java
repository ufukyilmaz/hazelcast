package com.hazelcast.wan.map;

import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Collection;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static java.util.Arrays.asList;

/**
 * WAN replication tests for hot-restart enabled maps.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWRHotRestartEnabledTest extends MapWanReplicationTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Test
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        configA.getMapConfig("default")
                .getHotRestartConfig()
                .setEnabled(true)
                .setFsync(false);
        configA.getHotRestartPersistenceConfig().setBaseDir(hotRestartFolderRule.getBaseDir());

        if (consistencyCheckStrategy == MERKLE_TREES) {
            configA.getMapConfig("default")
                    .setMerkleTreeConfig(new MerkleTreeConfig().setEnabled(true).setDepth(5));
            configB.getMapConfig("default")
                    .setMerkleTreeConfig(new MerkleTreeConfig().setEnabled(true).setDepth(5));
        }

        configA.getHotRestartPersistenceConfig()
                .setEnabled(true);
        startClusterA();
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
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
