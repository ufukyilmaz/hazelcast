package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.wan.WanSyncStatus;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanMerkleAntiEntropyTest {
    private static final String MAP_NAME = "MAP_WITH_MERKLETREES";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameterized.Parameters(name = "inMemoryFormat: {0} sourceDepth:{1} targetDepth:{2} partitions:{3} elements:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{

                // the cases below mostly work with very small record counts
                // this is because with this approach we can better approximate the expected partition difference count to the
                // real count

                // case: only one cluster member holds records to compare (no error on the other member)
                {OBJECT, 3, 3, 3, 1},
                {NATIVE, 3, 3, 3, 1},

                // case: higher record than partition count
                {OBJECT, 5, 5, 271, 1000},
                {NATIVE, 5, 5, 271, 1000},

                // the cases below test that:
                // - source and target Merkle trees have the same depth
                // - source Merkle trees are deeper than target trees
                // - source Merkle trees are shallower than target trees

                // case: even depth with smaller record than partition count
                {OBJECT, 3, 4, 5, 2},
                {OBJECT, 4, 4, 5, 2},
                {OBJECT, 4, 3, 5, 2},
                {NATIVE, 3, 4, 5, 2},
                {NATIVE, 4, 4, 5, 2},
                {NATIVE, 4, 3, 5, 2},

                // case: odd depth with smaller record than partition count
                {OBJECT, 5, 8, 7, 3},
                {OBJECT, 5, 5, 7, 3},
                {OBJECT, 8, 5, 7, 3},
                {NATIVE, 5, 8, 7, 3},
                {NATIVE, 5, 5, 7, 3},
                {NATIVE, 8, 5, 7, 3},

                // case: odd depths with highly unbalanced tree depths; smaller record than partition count
                {OBJECT, 5, 11, 271, 100},
                {OBJECT, 11, 11, 271, 100},
                {OBJECT, 11, 5, 271, 100},
                {NATIVE, 5, 11, 271, 100},
                {NATIVE, 11, 11, 271, 100},
                {NATIVE, 11, 5, 271, 100},
                });
    }

    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public int sourceTreeDepth;

    @Parameter(2)
    public int targetTreeDepth;

    @Parameter(3)
    public int partitions;

    @Parameter(4)
    public int entriesToPut;

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2).setup();
        targetCluster = clusterB(factory, 2).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(MERKLE_TREES)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.getConfig()
                     .getMapMerkleTreeConfig(MAP_NAME)
                     .setEnabled(true)
                     .setDepth(sourceTreeDepth);

        targetCluster.getConfig()
                     .getMapMerkleTreeConfig(MAP_NAME)
                     .setEnabled(true)
                     .setDepth(targetTreeDepth);

        sourceCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));
        targetCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));

        sourceCluster.getConfig().getMapConfig(MAP_NAME)
                     .setInMemoryFormat(inMemoryFormat);
        targetCluster.getConfig().getMapConfig(MAP_NAME)
                     .setInMemoryFormat(inMemoryFormat);

        if (inMemoryFormat == NATIVE) {
            sourceCluster.getConfig().getNativeMemoryConfig()
                         .setAllocatorType(STANDARD)
                         .setEnabled(true);

            targetCluster.getConfig().getNativeMemoryConfig()
                         .setAllocatorType(STANDARD)
                         .setEnabled(true);
        }
    }

    @Test
    public void testConsistencyCheckDifferences() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyConsistencyCheckResultExpectDifferences();
    }

    @Test
    public void testConsistencyCheckAllInSync() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        String valuePrefixCommonOnBothClusters = "T";
        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);
        fillMap(targetCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyConsistencyCheckResultExpectNoDifferences();
    }

    @Test
    public void testConsistencyCheckAfterReplicationAllInSync() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyConsistencyCheckResultExpectNoDifferences();
    }

    @Test
    public void testSyncPuts() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut);
        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        sourceCluster.syncMap(wanReplication, MAP_NAME);
        waitForSyncToComplete();

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyConsistencyCheckResultExpectNoDifferences();
    }

    @Ignore("Fails if a source partition is empty, but the target is not")
    @Test
    public void testMerkleSyncRemovals() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        String valuePrefixCommonOnBothClusters = "T";
        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);
        fillMap(targetCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);
        int randomInt = RandomPicker.getInt(0, entriesToPut);
        IMap<Object, Object> sourceMap = sourceCluster.getAMember().getMap(MAP_NAME);
        IMap<Object, Object> targetMap = targetCluster.getAMember().getMap(MAP_NAME);
        sourceMap.remove(randomInt);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        sourceCluster.syncMap(wanReplication, MAP_NAME);
        waitForSyncToComplete();

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyConsistencyCheckResultExpectNoDifferences();

        assertEquals(sourceMap.size(), targetMap.size());
        assertEquals(entriesToPut - 1, sourceMap.size());
    }

    private void verifyConsistencyCheckResultExpectDifferences() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int checkedPartitions = 0;
                int diffPartitions = 0;
                for (HazelcastInstance instance : sourceCluster.getMembers()) {
                    assertEquals(WanSyncStatus.READY, wanReplicationService(instance).getWanSyncState().getStatus());
                    Map<String, ConsistencyCheckResult> lastCheckResult = getLastCheckResult(instance, wanReplication);
                    ConsistencyCheckResult result = lastCheckResult.get(MAP_NAME);

                    assertNotNull(result);

                    checkedPartitions += result.getLastCheckedPartitionCount();
                    diffPartitions += result.getLastDiffPartitionCount();

                }

                assertEquals(partitions, checkedPartitions);
                assertTrue(diffPartitions <= partitions);
                assertTrue(diffPartitions <= entriesToPut);
                assertTrue(diffPartitions > 0);
            }
        });
    }

    private void verifyConsistencyCheckResultExpectNoDifferences() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int checkedPartitions = 0;
                int diffPartitions = 0;
                for (HazelcastInstance instance : sourceCluster.getMembers()) {
                    assertEquals(WanSyncStatus.READY, wanReplicationService(instance).getWanSyncState().getStatus());

                    Map<String, ConsistencyCheckResult> lastCheckResult = getLastCheckResult(instance, wanReplication);
                    ConsistencyCheckResult result = lastCheckResult.get(MAP_NAME);

                    assertNotNull(result);

                    checkedPartitions += result.getLastCheckedPartitionCount();
                    diffPartitions += result.getLastDiffPartitionCount();
                }

                assertEquals(partitions, checkedPartitions);
                assertEquals(0, diffPartitions);
            }
        });
    }

    private void waitForSyncToComplete() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean syncFinished = true;
                for (HazelcastInstance instance : sourceCluster.getMembers()) {
                    syncFinished &= wanReplicationService(instance).getWanSyncState().getStatus() == WanSyncStatus.READY;
                }
                assertTrue(syncFinished);
            }
        });
    }

    private Map<String, ConsistencyCheckResult> getLastCheckResult(HazelcastInstance instance, WanReplication wanReplication) {
        return wanReplicationService(instance)
                .getStats()
                .get(wanReplication.getSetupName()).getLocalWanPublisherStats()
                .get(wanReplication.getTargetClusterName()).getLastConsistencyCheckResults();
    }
}
