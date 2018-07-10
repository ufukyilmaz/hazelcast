package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

        assertKeysInEventually(singleNodeB, mapName1, 0, end);
        assertKeysInEventually(singleNodeB, mapName2, 0, end);

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

        assertKeysInEventually(clusterB, mapName, 0, 100);
        assertKeysInEventually(clusterC, mapName, 0, 100);

        createDataIn(clusterA, mapName, 0, 100, "dummy");
        removeDataIn(clusterA, mapName, 0, 100);

        assertKeysNotInEventually(clusterB, mapName, 0, 100);
        assertKeysNotInEventually(clusterC, mapName, 0, 100);

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

        //`dummy` value eventually makes it into the target map
        IMap<Integer, String> mapC = getMap(clusterC, mapName);
        assertKeyRangeMappedToValueEventually("dummy", 0, 100, mapC);

        // override the `dummy` value with different (generated) values in the source. these new values should not
        // make it into the target source.
        // this might sound counter-intuitive, but we are testing the PutIfAbsentMapMergePolicy here -> if the value
        // is already there then it won't be overwritten by a new value in the source cluster
        createDataIn(clusterB, mapName, 0, 100);

        // now run the same scenario, but use the 2nd source cluster and use different key range.
        // I am not very sure what is the reason for running the same scenario just with a different source cluster,
        // but I don't want to change it.
        createDataIn(clusterB, mapName, 100, 200, "dummy");
        assertKeyRangeMappedToValueEventually("dummy", 100, 200, mapC);
        createDataIn(clusterB, mapName, 100, 200);

        // ok, we now know both source clusters have the key-range 0..200 set to a generated value, but the target
        // cluster should still should have the keys mapped to 'dummy' as we have the Put-If-Absent policy
        assertKeyRangeMappedToValueAllTheTime("dummy", 0, 200, mapC, 10);

        assertWanQueuesEventuallyEmpty(clusterA, atocReplicationName, configC);
        assertWanQueuesEventuallyEmpty(clusterB, btocReplicationName, configC);
    }

    private static <V> void assertKeyRangeMappedToValue(V expectedValue, int rangeFromInclusive, int rangeToExclusive,
                                                        IMap<Integer, V> map) {
        for (int i = rangeFromInclusive; i < rangeToExclusive; i++) {
            V actualValue = map.get(i);
            assertEquals("Key '" + i + "' does not map to the expected value '" + expectedValue
                    + "' in the map '" + map + "'", expectedValue, actualValue);
        }
    }

    private static <V> void assertKeyRangeMappedToValueAllTheTime(final V expectedValue, final int rangeFromInclusive, final int rangeToExclusive,
                                                                  final IMap<Integer, V> map, int durationSeconds) {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertKeyRangeMappedToValue(expectedValue, rangeFromInclusive, rangeToExclusive, map);
            }
        }, durationSeconds);
    }

    private static <V> void assertKeyRangeMappedToValueEventually(final V expectedValue, final int rangeFromInclusive, final int rangeToExclusive,
                                                                  final IMap<Integer, V> map) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertKeyRangeMappedToValue(expectedValue, rangeFromInclusive, rangeToExclusive, map);
            }
        });
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
