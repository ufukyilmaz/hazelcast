package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.util.Collection;
import java.util.function.Supplier;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.getLastCheckResult;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.getNumberOfNonEmptyPartitions;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreConsistent;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreInconsistent;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

// This is a serial test because it runs 6 instances per test case
// and has 24 test cases that may run in parallel
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanMerkleHotRestartComplexTest extends HazelcastTestSupport {
    private static final String MAP_MERKLE1_NAME = "MAP_MERKLE";
    private static final String MAP_MERKLE2_NAME = "MAP_MERKLE2";
    private static final String MAP_NONMERKLE_NAME = "MAP_NONMERKLE";
    private static final String MERKLE1_REPLICATION_NAME = "wanReplicationMerkle1";
    private static final String MERKLE2_REPLICATION_NAME = "wanReplicationMerkle2";
    private static final String NON_MERKLE_REPLICATION_NAME = "wanReplicationNonMerkle";
    private static final int ONE_BACKUP = 1;
    private static final int TWO_BACKUPS = 2;
    private static final boolean HOT_RESTART_ENABLED = true;
    private static final boolean HOT_RESTART_DISABLED = false;
    private static final int SOURCE_MEMBERS_TWO = 2;
    private static final int SOURCE_MEMBERS_FOUR = 4;

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    protected File baseDir;

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplicationMerkle1;
    private WanReplication wanReplicationMerkle2;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameters(name = "inMemoryFormat: {0}")
    public static Collection<InMemoryFormat> parameters() {
        return asList(BINARY, OBJECT, NATIVE);
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @After
    public void tearDown() {
        sourceCluster.shutdownMembers();
        targetCluster.shutdownMembers();
        factory.terminateAll();
    }

    @Test
    public void testConsistencyCheckWithEmptyTargetCluster() {
        givenClusters(SOURCE_MEMBERS_TWO, HazelcastTestSupport::smallInstanceConfig);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE1_NAME, 0, 1000, "T");

        sourceCluster.bounceCluster();
        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);

        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME, 1000);
    }

    @Test
    public void testConsistencyCheckWithPartiallyFilledTargetCluster() {
        givenClusters(SOURCE_MEMBERS_TWO, HazelcastTestSupport::smallInstanceConfig);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE1_NAME, 0, 950, "T");

        sourceCluster.bounceCluster();
        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(950, targetCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);

        // 11 partitions with smallInstanceConfig
        // 32 leaves per partition since depth is 6
        // 352 leaves in total
        // expected difference is 50 entries
        // 50/352=~14% of the leaves is different at most
        // but multiple entries may share the same leaf
        // in practice the difference is <14
        int threshold = 15;
        verifyDeltaIsBelowThreshold(threshold);
    }

    @Test
    public void testConsistencyCheckWithFullyFilledTargetCluster() {
        givenClusters(SOURCE_MEMBERS_TWO, HazelcastTestSupport::smallInstanceConfig);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE1_NAME, 0, 1000, "T");

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        sourceCluster.bounceCluster();

        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);
    }

    @Test
    public void testConsistencyCheckWithFullyFilledTargetClusterAndTwoMerkleMaps() {
        givenClusters(SOURCE_MEMBERS_TWO, HazelcastTestSupport::smallInstanceConfig);

        wanReplicationMerkle2 = configureMerkleMap(MERKLE2_REPLICATION_NAME, MAP_MERKLE2_NAME, ONE_BACKUP, HOT_RESTART_ENABLED);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(sourceCluster, MAP_MERKLE2_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE2_NAME, 0, 1000, "T");

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        sourceCluster.consistencyCheck(wanReplicationMerkle2, MAP_MERKLE2_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle2, MAP_MERKLE2_NAME);

        sourceCluster.bounceCluster();

        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        sourceCluster.consistencyCheck(wanReplicationMerkle2, MAP_MERKLE2_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle2, MAP_MERKLE2_NAME);
    }

    @Test
    public void testConsistencyCheckWithFullyFilledTargetClusterAndTwoMerkleMaps_oneWithoutHotRestart() {
        givenClusters(SOURCE_MEMBERS_TWO, HazelcastTestSupport::smallInstanceConfig);

        wanReplicationMerkle2 = configureMerkleMap(MERKLE2_REPLICATION_NAME, MAP_MERKLE2_NAME, TWO_BACKUPS, HOT_RESTART_DISABLED);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE1_NAME, 0, 1000, "T");
        fillMap(sourceCluster, MAP_MERKLE2_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE2_NAME, 0, 1000, "T");

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        sourceCluster.consistencyCheck(wanReplicationMerkle2, MAP_MERKLE2_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle2, MAP_MERKLE2_NAME);

        sourceCluster.bounceCluster();

        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE1_NAME).size());
        assertEquals(0, sourceCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        int nonEmptyPartitionsInTarget = getNumberOfNonEmptyPartitions(targetCluster, MAP_MERKLE2_NAME);
        sourceCluster.consistencyCheck(wanReplicationMerkle2, MAP_MERKLE2_NAME);
        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplicationMerkle2, MAP_MERKLE2_NAME, nonEmptyPartitionsInTarget, 0);
    }

    @Test
    public void testConsistencyCheckWithFullyFilledTargetCluster_whenTwoSourceMembersTerminate() {
        Supplier<Config> configSupplier = () -> new Config()
                .setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(ClusterProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(ClusterProperty.EVENT_THREAD_COUNT.getName(), "1");

        givenClusters(SOURCE_MEMBERS_FOUR, configSupplier);

        wanReplicationMerkle2 = configureMerkleMap(MERKLE2_REPLICATION_NAME, MAP_MERKLE2_NAME, TWO_BACKUPS, HOT_RESTART_ENABLED);
        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_MERKLE2_NAME, 0, 1000, "T");
        fillMap(targetCluster, MAP_MERKLE2_NAME, 0, 1000, "T");

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE1_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE1_NAME);

        sourceCluster.bounceCluster();
        sourceCluster.getAMember().getLifecycleService().terminate();
        sourceCluster.getAMember().getLifecycleService().terminate();

        assertEquals(1000, sourceCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());
        assertEquals(1000, targetCluster.getAMember().getMap(MAP_MERKLE2_NAME).size());

        sourceCluster.consistencyCheck(wanReplicationMerkle1, MAP_MERKLE2_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplicationMerkle1, MAP_MERKLE2_NAME);
    }

    private void givenClusters(int sourceClusterSize, Supplier<Config> configSupplier) {
        sourceCluster = clusterA(factory, sourceClusterSize, configSupplier).setup();
        targetCluster = clusterB(factory, 1, configSupplier).setup();

        Config sourceConfig = sourceCluster.getConfig();
        Config targetConfig = targetCluster.getConfig();

        sourceConfig.getHotRestartPersistenceConfig()
                .setBaseDir(hotRestartFolderRule.getBaseDir())
                .setEnabled(true);

        if (inMemoryFormat == NATIVE) {
            sourceConfig.getNativeMemoryConfig()
                    .setAllocatorType(POOLED)
                    .setEnabled(true);

            targetConfig.getNativeMemoryConfig()
                    .setAllocatorType(POOLED)
                    .setEnabled(true);
        }

        wanReplicationMerkle1 = configureMerkleMap(MERKLE1_REPLICATION_NAME, MAP_MERKLE1_NAME, ONE_BACKUP, HOT_RESTART_ENABLED);

        configureNonMerkleMap();
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
                .setInMemoryFormat(inMemoryFormat)
                .getHotRestartConfig()
                .setEnabled(hotRestartEnabled);

        sourceConfig.getMapConfig(mapName)
                .setBackupCount(backupCount)
                .setAsyncBackupCount(0);

        sourceConfig
                .getMapConfig(mapName).getMerkleTreeConfig()
                .setEnabled(true)
                .setDepth(6);

        targetConfig.getMapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat);

        targetConfig.getMapConfig(mapName).getMerkleTreeConfig()
                .setEnabled(true)
                .setDepth(6);

        return wanReplication;
    }

    private WanReplication configureNonMerkleMap() {
        Config sourceConfig = sourceCluster.getConfig();
        Config targetConfig = targetCluster.getConfig();

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

        sourceConfig.getMapConfig(MAP_NONMERKLE_NAME)
                .setInMemoryFormat(inMemoryFormat);

        targetConfig.getMapConfig(MAP_NONMERKLE_NAME)
                .setInMemoryFormat(inMemoryFormat);

        return wanReplication;
    }

    private void verifyDeltaIsBelowThreshold(int threshold) {
        assertTrueEventually(() -> {
            ConsistencyCheckResult checkResult = getLastCheckResult(sourceCluster.getAMember(), wanReplicationMerkle1)
                    .get(MAP_MERKLE1_NAME);
            assertNotNull(checkResult);
            float diffPercentage = checkResult.getDiffPercentage();
            assertTrue("Map difference percentage is " + diffPercentage, diffPercentage < threshold);
        });
    }
}
