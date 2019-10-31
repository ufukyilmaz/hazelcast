package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.WanSyncStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanMerkleSyncCountTest {
    private static final String MAP_NAME = "MAP_WITH_MERKLETREES";
    private static final String REPLICATION_NAME = "wanReplication";

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameters(name = "sourceDepth: {0} targetDepth: {1} partitions: {2} sourceCount: {3} targetCount:{4} syncCount:{5}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                // depths are equal on both sides - local finishes comparison
                {4, 4, 1, 10, 9, 2},
                // local deeper - remote finishes comparison
                {5, 4, 1, 10, 9, 1},
                // remote deeper - local finishes comparison
                {4, 5, 1, 10, 9, 2},

                // source and target is identical, nothing is synchronized
                {12, 12, 271, 1000, 1000, 0},
                // target is empty, everything is synchronized
                {12, 12, 271, 1000, 0, 1000},

                // tests verify if the number of the synchronized entries shows the expected efficiency
                // expected syncCount is set in an empirical way, since the distribution of the records between
                // the leaves is not perfect
                {12, 12, 271, 10000, 9500, 509},
                {12, 12, 271, 1000, 950, 50},
                {12, 12, 271, 1000, 750, 251},
                {12, 12, 271, 1000, 500, 500},
                {12, 12, 271, 1000, 250, 750},

                // this test case shows that the imperfect key distribution between the leaves may lead to unexpected inefficiency
                // in this case one would naively expect that we transfer around 104 (2093-1989) entries, but we transfer 214
                //
                // we have 12 deep Merkle trees on both sides, which is 2048 leaves for the 2093 entries
                // with a perfect key distribution we would synchronize from 104 leaves records between 104 and 149 (104+(2093-2048))
                // - 104 would be the lucky case where we synchronize from leaves that hold only one record
                //   in this case the avg keys per inconsistent leaves would be 1
                // - 149 would be the worst case where we synchronize from all (45) leaves that hold 2 entries and the rest
                //   from leaves hold only 1 entries
                //   in this case the avg keys per inconsistent leaves would be ~1.433
                // but in this given case the avg keys per inconsistent leaves is ~2.099
                {12, 12, 1, 2093, 1989, 214}
        });
    }

    @Parameter(0)
    public int sourceDepth;

    @Parameter(1)
    public int targetDepth;

    @Parameter(2)
    public int partitions;

    @Parameter(3)
    public int sourceCount;

    @Parameter(4)
    public int targetCount;

    @Parameter(5)
    public int expectedSyncCount;

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
                .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                .setEnabled(true)
                .setDepth(sourceDepth);

        targetCluster.getConfig()
                .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                .setEnabled(true)
                .setDepth(sourceDepth);

        sourceCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));
        targetCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));
    }

    @Test
    public void testSyncCount() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        String valuePrefixCommonOnBothClusters = "T";
        fillMap(sourceCluster, MAP_NAME, 0, sourceCount, valuePrefixCommonOnBothClusters);
        fillMap(targetCluster, MAP_NAME, 0, targetCount, valuePrefixCommonOnBothClusters);

        sourceCluster.syncMap(wanReplication, MAP_NAME);
        waitForSyncToComplete(sourceCluster);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifySyncResult(expectedSyncCount);
    }

    private void verifySyncResult(final int expectedRecordsToSync) {
        assertTrueEventually(() -> {
            int syncCount = 0;
            for (HazelcastInstance instance : sourceCluster.getMembers()) {
                EnterpriseWanReplicationService replicationService = wanReplicationService(instance);
                assertEquals(WanSyncStatus.READY, replicationService.getWanSyncState().getStatus());

                LocalWanStats localWanStats = replicationService.getStats().get(wanReplication.getSetupName());
                DistributedServiceWanEventCounters.DistributedObjectWanEventCounters counters = localWanStats
                        .getLocalWanPublisherStats().get(targetCluster.getName()).getSentMapEventCounter().get(MAP_NAME);

                if (counters != null) {
                    syncCount += counters.getSyncCount();
                }
            }

            assertTrue("Sync count " + syncCount + " should == " + expectedRecordsToSync, syncCount == expectedRecordsToSync);
        });
    }

}
