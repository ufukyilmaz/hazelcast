package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanMerkleHotRestartSimpleTest extends HazelcastTestSupport {
    private static final String MAP_MERKLE1_NAME = "MAP_MERKLE";
    private static final String MAP_NONMERKLE_NAME = "MAP_NONMERKLE";
    private static final String MERKLE_REPLICATION_NAME = "wanReplicationMerkle";
    private static final String NON_MERKLE_REPLICATION_NAME = "wanReplicationNonMerkle";
    private static final int ONE_BACKUP = 1;
    private static final boolean HOT_RESTART_ENABLED = true;

    protected File baseDir;

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Before
    public void setupWithMerkleTree() {
        sourceCluster = clusterA(factory, 1).setup();
        targetCluster = clusterB(factory, 1).setup();

        Config sourceConfig = sourceCluster.getConfig();

        sourceConfig.getHotRestartPersistenceConfig()
                    .setBaseDir(hotRestartFolderRule.getBaseDir())
                    .setEnabled(true);

        configureMerkleMap(MERKLE_REPLICATION_NAME, MAP_MERKLE1_NAME, ONE_BACKUP, HOT_RESTART_ENABLED);

        configureNonMerkleMap();
    }

    @After
    public void tearDown() {
        sourceCluster.shutdownMembers();
        targetCluster.shutdownMembers();
        factory.terminateAll();
    }

    private WanReplication configureMerkleMap(String wanReplicationName, String mapName, int backupCount,
                                              boolean hotRestartEnabled) {
        Config sourceConfig = sourceCluster.getConfig();
        Config targetConfig = targetCluster.getConfig();

        WanReplication wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(wanReplicationName)
                .withConsistencyCheckStrategy(MERKLE_TREES)
                .withInitialPublisherState(WanPublisherState.STOPPED)
                .setup();

        sourceCluster.replicateMap(mapName)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceConfig.getMapConfig(mapName)
                    .getHotRestartConfig()
                    .setEnabled(hotRestartEnabled);

        sourceConfig.getMapConfig(mapName)
                    .setBackupCount(backupCount)
                    .setAsyncBackupCount(0);

        sourceConfig
                .getMapMerkleTreeConfig(mapName)
                .setEnabled(true)
                .setDepth(6);

        targetConfig.getMapMerkleTreeConfig(mapName)
                    .setEnabled(true)
                    .setDepth(6);

        return wanReplication;
    }

    private WanReplication configureNonMerkleMap() {
        Config sourceConfig = sourceCluster.getConfig();

        WanReplication wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(NON_MERKLE_REPLICATION_NAME)
                .withInitialPublisherState(WanPublisherState.STOPPED)
                .setup();

        sourceCluster.replicateMap(MAP_NONMERKLE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceConfig.getMapConfig(MAP_NONMERKLE_NAME)
                    .getHotRestartConfig()
                    .setEnabled(true);

        return wanReplication;
    }

    @Test
    public void testHotRestartWithNonMerkleMapDataSucceeds() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NONMERKLE_NAME, 0, 1000);

        sourceCluster.bounceCluster();
    }

    @Test
    public void testHotRestartWithNoDataSucceeds() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.bounceCluster();
    }
}
