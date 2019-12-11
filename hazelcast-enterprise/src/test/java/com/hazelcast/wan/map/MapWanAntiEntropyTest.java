package com.hazelcast.wan.map;

import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.wan.WanPublisherState.STOPPED;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.getLastCheckResults;
import static com.hazelcast.wan.fw.WanCounterTestSupport.getClusterWideSumPartitionsSyncedCount;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.map.WanBatchPublisherMapTest.waitForSyncToComplete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanAntiEntropyTest extends HazelcastTestSupport {

    @Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<ConsistencyCheckStrategy> parameters() {
        return asList(NONE, MERKLE_TREES);
    }

    private static final String MAP_REPLICATED = "mapReplicated";
    private static final String MAP_NOT_REPLICATED = "mapNotReplicated";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;
    private WanReplication wanReplication;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        clusterA = clusterA(factory, 1).setup();
        clusterB = clusterB(factory, 1).setup();

        configureMerkleTrees(clusterA);
        configureMerkleTrees(clusterB);

        wanReplication = replicate()
                .from(clusterA)
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withInitialPublisherState(STOPPED)
                .setup();

        clusterA.replicateMap(MAP_REPLICATED)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();
    }

    private void configureMerkleTrees(Cluster cluster) {
        if (consistencyCheckStrategy == MERKLE_TREES) {
            cluster.getConfig()
                    .getMapConfig("default").getMerkleTreeConfig()
                    .setEnabled(true)
                    .setDepth(6);
        }
    }

    @Test
    public void testSyncingNotReplicatedMapIsIgnored() {
        givenSourceClusterWithData();

        clusterA.syncMap(wanReplication, MAP_NOT_REPLICATED);

        waitForSyncToComplete(clusterA.getMembers());
        int syncedPartitions = getClusterWideSumPartitionsSyncedCount(clusterA, wanReplication, MAP_NOT_REPLICATED);
        assertEquals(0, syncedPartitions);
    }

    @Test
    public void testSyncingAllMapsExcludesNotReplicatedMap() {
        givenSourceClusterWithData();

        clusterA.syncAllMaps(wanReplication);

        assertTrueEventually(() -> {
            int syncedPartitionsNotReplicated = getClusterWideSumPartitionsSyncedCount(clusterA, wanReplication,
                    MAP_NOT_REPLICATED);
            assertEquals(0, syncedPartitionsNotReplicated);

            int syncedPartitionsReplicated = getClusterWideSumPartitionsSyncedCount(clusterA, wanReplication, MAP_REPLICATED);
            assertTrue(syncedPartitionsReplicated != 0);
        });
        verifyMapReplicated(clusterA, clusterB, MAP_REPLICATED);
    }

    @Test
    public void testSyncingReplicatedMapIsPerformed() {
        givenSourceClusterWithData();

        clusterA.syncMap(wanReplication, MAP_REPLICATED);

        assertTrueEventually(() -> {
            int syncedPartitions = getClusterWideSumPartitionsSyncedCount(clusterA, wanReplication, MAP_REPLICATED);
            assertTrue(syncedPartitions != 0);
        });
        verifyMapReplicated(clusterA, clusterB, MAP_REPLICATED);
    }

    @Test
    public void testConsistencyCheckOfNotReplicatedMapIsIgnored() {
        assumeTrue(consistencyCheckStrategy == MERKLE_TREES);

        givenSourceClusterWithData();

        clusterA.consistencyCheck(wanReplication, MAP_NOT_REPLICATED);

        waitForSyncToComplete(clusterA.getMembers());
        Collection<Map<String, ConsistencyCheckResult>> checkResults = getLastCheckResults(clusterA, wanReplication);
        for (Map<String, ConsistencyCheckResult> memberResults : checkResults) {
            assertNull(memberResults.get(MAP_NOT_REPLICATED));
            assertNull(memberResults.get(MAP_REPLICATED));
        }
    }

    @Test
    public void testConsistencyCheckOfReplicatedMapIsPerformed() {
        assumeTrue(consistencyCheckStrategy == MERKLE_TREES);

        givenSourceClusterWithData();

        clusterA.consistencyCheck(wanReplication, MAP_REPLICATED);

        waitForSyncToComplete(clusterA.getMembers());
        Collection<Map<String, ConsistencyCheckResult>> checkResults = getLastCheckResults(clusterA, wanReplication);
        for (Map<String, ConsistencyCheckResult> memberResults : checkResults) {
            assertNull(memberResults.get(MAP_NOT_REPLICATED));
            assertNotNull(memberResults.get(MAP_REPLICATED));
        }
    }

    private void givenSourceClusterWithData() {
        clusterA.startClusterAndWaitForSafeState();
        clusterB.startCluster();
        fillMap(clusterA, MAP_REPLICATED, 0, 10);
        fillMap(clusterA, MAP_NOT_REPLICATED, 0, 10);
    }
}
