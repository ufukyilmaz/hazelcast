package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.MerkleTreeWanSyncStats;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.sync.SyncFailedException;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanSyncStats;
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
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapWanSyncAPITest extends MapWanReplicationTestSupport {

    @Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Override
    protected Config getConfig() {
        final Config config = super.getConfig();
        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());

        if (consistencyCheckStrategy == MERKLE_TREES) {
            config.getMapMerkleTreeConfig("default")
                  .setEnabled(true)
                  .setDepth(6);
        }

        return config;
    }

    @Test
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        // create another map to verify that only one map is synced even if there are multiple ones
        createDataIn(clusterA, "map2", 0, 10);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
        wanReplicationService.syncMap("atob", "B", "map");

        assertKeysInEventually(clusterB, "map", 0, 1000);
        verifySyncStats(clusterA, "atob", "B", "map");
    }

    @Test
    public void syncAllTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        createDataIn(clusterA, "map3", 0, 3000);

        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterB, "map2", 0, 2000, clusterA);
        assertDataInFromEventually(clusterB, "map3", 0, 3000, clusterA);

        clusterB[0].getCluster().shutdown();
        startClusterB();

        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterB, "map2", 0, 2000);
        assertKeysNotInEventually(clusterB, "map3", 0, 3000);

        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
        wanReplicationService.syncAllMaps("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
        verifySyncStats(clusterA, "atob", "B", "map", "map2", "map3");
    }

    @Test
    public void syncAllAfterAddingMemberToSourceCluster() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        configA.setInstanceName(configA.getInstanceName() + 0);
        clusterA = new HazelcastInstance[]{factory.newHazelcastInstance(configA)};
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        createDataIn(clusterA, "map3", 0, 3000);

        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterB, "map2", 0, 2000, clusterA);
        assertDataInFromEventually(clusterB, "map3", 0, 3000, clusterA);

        clusterB[0].getCluster().shutdown();
        startClusterB();

        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA = new HazelcastInstance[]{clusterA[0], factory.newHazelcastInstance(configA)};

        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterB, "map2", 0, 2000);
        assertKeysNotInEventually(clusterB, "map3", 0, 3000);

        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
        wanReplicationService.syncAllMaps("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
        verifySyncStats(clusterA, "atob", "B", "map", "map2", "map3");
    }


    @Test(expected = SyncFailedException.class)
    public void sendMultipleSyncRequests() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        getWanReplicationService(clusterA[0]).syncMap("atob", configB.getGroupConfig().getName(), "map");
        getWanReplicationService(clusterA[0]).syncMap("atob", configB.getGroupConfig().getName(), "map");
    }

    private static Map<String, WanSyncStats> getLastSyncResult(HazelcastInstance instance, String setupName, String publisherId) {
        return wanReplicationService(instance)
                .getStats()
                .get(setupName).getLocalWanPublisherStats()
                .get(publisherId).getLastSyncStats();
    }

    private void verifySyncStats(HazelcastInstance[] cluster, String setupName, String publisherId, String... mapNames) {
        for (HazelcastInstance instance : cluster) {
            Map<String, WanSyncStats> lastSyncResult = getLastSyncResult(instance, setupName, publisherId);
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
    }

    private int getLocalPartitions(HazelcastInstance instance) {
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

    private void verifyFullSyncStats(Map<String, WanSyncStats> lastSyncResult, int localPartitions, String mapName) {
        WanSyncStats wanSyncStats = lastSyncResult.get(mapName);

        assertEquals(localPartitions, wanSyncStats.getPartitionsSynced());
        assertTrue(wanSyncStats.getRecordsSynced() > 0);
    }

    private void verifyMerkleSyncStats(Map<String, WanSyncStats> lastSyncResult, int localPartitions, String mapName) {
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

}
