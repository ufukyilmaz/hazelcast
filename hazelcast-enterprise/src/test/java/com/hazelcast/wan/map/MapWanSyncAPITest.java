package com.hazelcast.wan.map;

import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.replication.MerkleTreeWanSyncStats;
import com.hazelcast.enterprise.wan.impl.sync.SyncFailedException;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanSyncStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.getPartitionService;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static com.hazelcast.wan.map.MapWanBatchReplicationTest.isAllMembersConnected;
import static com.hazelcast.wan.map.MapWanReplicationTestSupport.assertKeysNotInEventually;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanSyncAPITest extends HazelcastTestSupport {

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    @Parameters(name = "consistencyCheckStrategy:{0}, maxConcurrentInvocations:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE, -1},
                {NONE, 100},
                {MERKLE_TREES, -1},
                {MERKLE_TREES, 100}
        });
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();

        configureMerkleTrees(clusterA);
        configureMerkleTrees(clusterB);

        WanReplication wanReplication = replicate()
                .from(clusterA)
                .to(null)
                .withSetupName(REPLICATION_NAME)
                .setup();


        clusterA.replicateMap("default")
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

//         uncomment to dump the counters when debugging locally
        //dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureMerkleTrees(Cluster cluster) {
        if (consistencyCheckStrategy == MERKLE_TREES) {
            cluster.getConfig()
                    .getMapConfig("default").getMerkleTreeConfig()
                    .setEnabled(true)
                    .setDepth(6);
        }
    }

    @Parameter(0)
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Parameter(1)
    public int maxConcurrentInvocations;

    @Test
    public void basicSyncTest() {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .setup();
        clusterA.addWanReplication(toBReplication);

        fillMap(clusterA, MAP_NAME, 0, 1000);
        verifyMapReplicated(clusterA, clusterB, MAP_NAME);

        // create another map to verify that only one map is synced even if there are multiple ones
        fillMap(clusterA, "notSynced", 0, 1000);

        clusterB.shutdownMembers();
        factory.cleanup();
        clusterB.startCluster();

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);

        clusterA.syncMap(toBReplication, MAP_NAME);

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            // In this case, this following sync might not actually sync any records, causing the
            // verifySyncStats to fail.
            // This is an edge case and in most cases this branch will not be taken. Even so,
            // the more important assertion is actually verifying the map is replicated.
            clusterA.syncMap(toBReplication, MAP_NAME);
        } else {
            // in case we did only one sync, there should be some non-zero sync stats
            verifySyncStats(clusterA, toBReplication, MAP_NAME);
        }

        verifyMapReplicated(clusterA, clusterB, MAP_NAME);
    }

    @Test
    public void syncAllTest() {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .setup();
        clusterA.addWanReplication(toBReplication);

        fillMap(clusterA, "map", 0, 1000);
        fillMap(clusterA, "map2", 0, 2000);
        fillMap(clusterA, "map3", 0, 3000);

        verifyMapReplicated(clusterA, clusterB, "map");
        verifyMapReplicated(clusterA, clusterB, "map2");
        verifyMapReplicated(clusterA, clusterB, "map3");

        clusterB.shutdownMembers();
        factory.cleanup();
        clusterB.startCluster();

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);
        assertKeysNotInEventually(clusterB.getMembers(), "map2", 0, 2000);
        assertKeysNotInEventually(clusterB.getMembers(), "map3", 0, 3000);

        clusterA.syncAllMaps(toBReplication);

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            clusterA.syncAllMaps(toBReplication);
        }

        verifyMapReplicated(clusterA, clusterB, "map");
        verifyMapReplicated(clusterA, clusterB, "map2");
        verifyMapReplicated(clusterA, clusterB, "map3");

        verifySyncStats(clusterA, toBReplication, "map", "map2", "map3");
    }

    @Test
    public void addNewWanConfigAndSyncTest() {
        clusterA.startCluster();
        clusterB.startCluster();

        fillMap(clusterA, "map", 0, 1000);
        fillMap(clusterA, "map2", 0, 2000);

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);

        String newReplicationName = "newReplicationName";
        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .withSetupName(newReplicationName)
                .setup();

        AddWanConfigResult result = clusterA.addWanReplication(toBReplication);
        assertContains(result.getAddedPublisherIds(), clusterB.getName());
        assertEquals(0, result.getIgnoredPublisherIds().size());

        fillMap(clusterA, "map3", 0, 3000);

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);
        assertKeysNotInEventually(clusterB.getMembers(), "map2", 0, 2000);
        assertKeysNotInEventually(clusterB.getMembers(), "map3", 0, 3000);

        clusterA.syncAllMaps(toBReplication);

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), newReplicationName, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            clusterA.syncAllMaps(toBReplication);
        }

        verifyMapReplicated(clusterA, clusterB, "map");
        verifyMapReplicated(clusterA, clusterB, "map2");
        verifyMapReplicated(clusterA, clusterB, "map3");
    }

    @Test
    public void sendMultipleSyncRequests() {
        clusterA.startAClusterMember();
        fillMap(clusterA, MAP_NAME, 0, 10000);

        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .withReplicationBatchSize(1)
                .setup();
        clusterA.addWanReplication(toBReplication);

        clusterA.syncAllMaps(toBReplication);

        expectedException.expect(
                new RootCauseMatcher(SyncFailedException.class, "Another anti-entropy request is already in progress."));
        clusterA.syncAllMaps(toBReplication);
    }

    @Test
    public void tryToSyncNonExistingConfig() {
        clusterA.startCluster();
        final WanReplication nonExistantReplication = replicate()
                .to(clusterB)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .withSetupName(REPLICATION_NAME)
                .setup();

        expectedException.expect(new RootCauseMatcher(InvalidConfigurationException.class,
                "WAN Replication Config doesn't exist with WAN configuration name wanReplication and publisher ID B"));
        clusterA.syncMap(nonExistantReplication, MAP_NAME);
    }

    @Test
    public void checkWanSyncState() {
        clusterA.startCluster();
        clusterB.startCluster();
        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .withSetupName(REPLICATION_NAME)
                .setup();
        AddWanConfigResult result = clusterA.addWanReplication(toBReplication);
        assertContains(result.getAddedPublisherIds(), clusterB.getName());
        assertEquals(0, result.getIgnoredPublisherIds().size());

        fillMap(clusterA, MAP_NAME, 0, 10000);
        verifyMapReplicated(clusterA, clusterB, MAP_NAME);

        clusterB.shutdownMembers();
        factory.cleanup();

        clusterA.syncMap(toBReplication, MAP_NAME);
        assertSyncState(clusterA, toBReplication, WanSyncStatus.IN_PROGRESS, -1);

        clusterB.startCluster();

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            clusterA.syncMap(toBReplication, MAP_NAME);
        }
        waitForSyncToComplete(clusterA);

        verifyMapReplicated(clusterA, clusterB, MAP_NAME);

        assertSyncState(clusterA, toBReplication, WanSyncStatus.READY,
                getPartitionService(clusterA.getAMember()).getPartitionCount());
    }

    private static Map<String, WanSyncStats> getLastSyncResult(HazelcastInstance instance, String setupName, String publisherId) {
        return wanReplicationService(instance)
                .getStats()
                .get(setupName).getLocalWanPublisherStats()
                .get(publisherId).getLastSyncStats();
    }

    private void verifySyncStats(Cluster sourceCluster,
                                 WanReplication wanReplication,
                                 String... mapNames) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : sourceCluster.getMembers()) {
                Map<String, WanSyncStats> lastSyncResult = getLastSyncResult(
                        instance, wanReplication.getSetupName(), wanReplication.getTargetClusterName());
                int localPartitions = getLocalPartitions(instance);

                for (String mapName : mapNames) {
                    assertTrue(lastSyncResult.containsKey(mapName));

                    if (consistencyCheckStrategy == NONE) {
                        verifyFullSyncStats(lastSyncResult, localPartitions, mapName);
                    } else if (consistencyCheckStrategy == MERKLE_TREES) {
                        verifyMerkleSyncStats(lastSyncResult, localPartitions, mapName);
                    } else {
                        fail("Unhandled consistency check strategy");
                    }
                }
            }
        });
    }

    private static int getLocalPartitions(HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int localPartitions = 0;
        IPartition[] partitions = nodeEngineImpl.getPartitionService().getPartitions();
        for (IPartition partition : partitions) {
            if (partition.isLocal()) {
                localPartitions++;
            }
        }
        return localPartitions;
    }

    private static void verifyFullSyncStats(
            Map<String, WanSyncStats> lastSyncResult, int localPartitions, String mapName) {
        WanSyncStats wanSyncStats = lastSyncResult.get(mapName);

        assertEquals(localPartitions, wanSyncStats.getPartitionsSynced());
        assertTrue(wanSyncStats.getRecordsSynced() > 0);
    }

    private static void verifyMerkleSyncStats(
            Map<String, WanSyncStats> lastSyncResult, int localPartitions, String mapName) {
        MerkleTreeWanSyncStats wanSyncStats = (MerkleTreeWanSyncStats) lastSyncResult.get(mapName);

        // we don't assert on actual values here just verify if the values are filled as expected
        assertTrue(wanSyncStats.getDurationSecs() >= 0);
        assertTrue(wanSyncStats.getPartitionsSynced() > 0);
        assertTrue(wanSyncStats.getRecordsSynced() > 0);
        assertTrue(wanSyncStats.getNodesSynced() > 0);
        assertTrue(wanSyncStats.getAvgEntriesPerLeaf() > 0);
        assertTrue(wanSyncStats.getStdDevEntriesPerLeaf() > 0);
        assertTrue(wanSyncStats.getMinLeafEntryCount() > 0);
        assertTrue(wanSyncStats.getMaxLeafEntryCount() > 0);
    }

    /**
     * Asserts that at least one member in the {@code cluster} will have the
     * provided {@code syncStatus} for the {@code wanReplication}. This is
     * because the sync state is only kept on one member (which initiated
     * the sync).
     * If the {@code expectedSyncedPartitionCount} is less than {@code 0}, the
     * check for total synced partition count will be skipped.
     *
     * @param cluster        the source cluster
     * @param wanReplication the WAN replication
     * @param syncStatus     the expected sync status
     */
    private static void assertSyncState(Cluster cluster,
                                        WanReplication wanReplication,
                                        WanSyncStatus syncStatus,
                                        int expectedSyncedPartitionCount) {
        boolean passed = false;
        ArrayList<WanSyncState> wanSyncStates = new ArrayList<WanSyncState>(cluster.size());
        int totalSyncedPartitions = 0;

        for (HazelcastInstance instance : cluster.getMembers()) {
            WanSyncState syncState = wanReplicationService(instance).getWanSyncState();
            boolean memberSyncStatePasses =
                    syncState.getStatus() == syncStatus
                            && wanReplication.getSetupName().equals(syncState.getActiveWanConfigName())
                            && wanReplication.getTargetClusterName().equals(syncState.getActivePublisherName());
            passed |= memberSyncStatePasses;
            wanSyncStates.add(syncState);
            totalSyncedPartitions += syncState.getSyncedPartitionCount();
        }
        assertTrue("Expected one cluster member to have syncStatus " + syncStatus + " but statuses were " + wanSyncStates,
                passed);

        if (expectedSyncedPartitionCount > 0) {
            assertEquals("Expected " + totalSyncedPartitions + " but sync states were " + wanSyncStates,
                    expectedSyncedPartitionCount, totalSyncedPartitions);
        }
    }
}
