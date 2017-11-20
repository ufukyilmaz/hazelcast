package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.WanReplicationService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class MapWanBatchReplicationSnapshotTest extends MapWanReplicationTestSupport {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Override
    protected boolean isSnapshotEnabled() {
        return true;
    }

    @Test
    public void testMutationOnDifferentMapsWithSameKeys() {
        final String replicaName = "multiReplica";
        final String mapName1 = "map1";
        final String mapName2 = "map2";
        setupReplicateFrom(configA, configB, singleNodeB.length, replicaName, PassThroughMergePolicy.class.getName());
        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);

        final IMap<Integer, String> m1 = singleNodeA[0].getMap(mapName1);
        final IMap<Integer, String> m2 = singleNodeA[0].getMap(mapName2);
        final int end = 1000;
        for (int i = 0; i < end; i++) {
            final String value = configA.getGroupConfig().getName() + i;
            m1.put(i, value + 2);
            m1.put(i, value + 1);
            m1.put(i, value);
            m2.put(i, value + 2);
            m2.put(i, value + 1);
            m2.put(i, value);
        }

        assertKeysIn(singleNodeB, mapName1, 0, end);
        assertKeysIn(singleNodeB, mapName2, 0, end);

        assertWanQueuesEventuallyEmpty(singleNodeA, replicaName, configB);
    }

    @Test
    public void VTopo_2passiveReplica_1producer() {
        final String replicaName = "multiReplica";
        final String mapName = "map";
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, mapName, 0, 100, "dummy");
        createDataIn(clusterA, mapName, 0, 100);

        assertKeysIn(clusterB, mapName, 0, 100);
        assertKeysIn(clusterC, mapName, 0, 100);

        createDataIn(clusterA, mapName, 0, 100, "dummy");
        removeDataIn(clusterA, mapName, 0, 100);

        assertKeysNotIn(clusterB, mapName, 0, 100);
        assertKeysNotIn(clusterC, mapName, 0, 100);

        assertDataSizeEventually(clusterB, mapName, 0);
        assertDataSizeEventually(clusterC, mapName, 0);

        assertWanQueuesEventuallyEmpty(clusterA, replicaName, configB);
        assertWanQueuesEventuallyEmpty(clusterA, replicaName, configC);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withPutIfAbsentMapMergePolicy() {
        final String atocReplicationName = "atoc";
        final String btocReplicationName = "btoc";
        final String mapName = "map";
        setupReplicateFrom(configA, configC, clusterC.length, atocReplicationName, PutIfAbsentMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, btocReplicationName, PutIfAbsentMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, mapName, 0, 100, "dummy");
        createDataIn(clusterA, mapName, 0, 100);

        createDataIn(clusterB, mapName, 100, 200, "dummy");
        createDataIn(clusterB, mapName, 100, 200);

        assertDataInFrom(clusterC, mapName, 0, 100, clusterA);
        assertDataInFrom(clusterC, mapName, 100, 200, clusterB);

        createDataIn(clusterB, mapName, 0, 100);
        assertDataInFrom(clusterC, mapName, 0, 100, clusterA);

        assertDataSizeEventually(clusterC, mapName, 200);

        assertWanQueuesEventuallyEmpty(clusterA, atocReplicationName, configC);
        assertWanQueuesEventuallyEmpty(clusterB, btocReplicationName, configC);
    }

    private void assertWanQueuesEventuallyEmpty(final HazelcastInstance[] nodes,
                                                final String wanReplicationName,
                                                final Config toConfig) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance node : nodes) {
                    final WanReplicationService service = getNodeEngineImpl(node).getService(WanReplicationService.SERVICE_NAME);
                    final LocalWanStats localWanStats = service.getStats().get(wanReplicationName);
                    final LocalWanPublisherStats publisherStats = localWanStats.getLocalWanPublisherStats()
                                                                               .get(toConfig.getGroupConfig().getName());
                    final int actualQueueSize = publisherStats.getOutboundQueueSize();
                    assertEquals(0, actualQueueSize);
                }
            }
        });
    }
}
