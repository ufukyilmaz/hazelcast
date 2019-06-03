package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.wan.WanSyncStatus;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;

import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WanAntiEntropyTestSupport {
    private WanAntiEntropyTestSupport() {
    }

    public static void verifyAllPartitionsAreInconsistent(final Cluster cluster,
                                                          final WanReplication wanReplication,
                                                          final String mapName) {
        int nonEmptyPartitions = getNumberOfNonEmptyPartitions(cluster, mapName);
        verifyAllPartitionsAreInconsistent(cluster, wanReplication, mapName, nonEmptyPartitions, -1);
    }

    public static void verifyAllPartitionsAreInconsistent(final Cluster cluster,
                                                          final WanReplication wanReplication,
                                                          final String mapName,
                                                          final int expectedEntriesToSync) {
        int nonEmptyPartitions = getNumberOfNonEmptyPartitions(cluster, mapName);
        verifyAllPartitionsAreInconsistent(cluster, wanReplication, mapName, nonEmptyPartitions, expectedEntriesToSync);
    }

    public static void verifyAllPartitionsAreInconsistent(final Cluster cluster,
                                                          final WanReplication wanReplication,
                                                          final String mapName,
                                                          final int inconsistentPartitions,
                                                          final int expectedEntriesToSync) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int checkedPartitions = 0;
                int diffPartitions = 0;
                int entriesToSync = 0;
                for (HazelcastInstance instance : cluster.getMembers()) {
                    assertEquals(WanSyncStatus.READY, wanReplicationService(instance).getWanSyncState().getStatus());
                    Map<String, ConsistencyCheckResult> lastCheckResult = getLastCheckResult(instance, wanReplication);
                    ConsistencyCheckResult result = lastCheckResult.get(mapName);

                    assertNotNull(result);

                    checkedPartitions += result.getLastCheckedPartitionCount();
                    diffPartitions += result.getLastDiffPartitionCount();
                    entriesToSync += result.getLastEntriesToSync();
                }

                int partitions = cluster.getAMember().getPartitionService().getPartitions().size();
                assertEquals("All partitions are expected to be checked by the Merkle comparison",
                        partitions, checkedPartitions);
                assertEquals("Partitions with differences should be equal to " + inconsistentPartitions,
                        inconsistentPartitions, diffPartitions);
                assertEquals("Entries to synchronize should be equal to " + expectedEntriesToSync, expectedEntriesToSync,
                        entriesToSync);
            }
        });
    }

    public static void verifyAllPartitionsAreConsistent(final Cluster cluster,
                                                        final WanReplication wanReplication,
                                                        final String mapName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int partitions = cluster.getAMember().getPartitionService().getPartitions().size();
                int checkedPartitions = 0;
                int diffPartitions = 0;
                int entriesToSync = 0;
                for (HazelcastInstance instance : cluster.getMembers()) {
                    if (instance.getLifecycleService().isRunning()) {
                        assertEquals(WanSyncStatus.READY, wanReplicationService(instance).getWanSyncState().getStatus());

                        Map<String, ConsistencyCheckResult> lastCheckResult = getLastCheckResult(instance, wanReplication);
                        assertNotNull(lastCheckResult);

                        ConsistencyCheckResult result = lastCheckResult.get(mapName);
                        assertNotNull(result);

                        checkedPartitions += result.getLastCheckedPartitionCount();
                        diffPartitions += result.getLastDiffPartitionCount();
                        entriesToSync += result.getLastEntriesToSync();
                    }
                }

                assertEquals(partitions, checkedPartitions);
                assertEquals(0, diffPartitions);
                assertEquals(0, entriesToSync);
            }
        });
    }

    public static Map<String, ConsistencyCheckResult> getLastCheckResult(HazelcastInstance instance,
                                                                         WanReplication wanReplication) {
        return wanReplicationService(instance)
                .getStats()
                .get(wanReplication.getSetupName()).getLocalWanPublisherStats()
                .get(wanReplication.getTargetClusterName()).getLastConsistencyCheckResults();
    }

    public static int getNumberOfNonEmptyPartitions(Cluster cluster, String mapName) {
        // the record store size is updated on the partition threads, we may read stale values here
        // but since this method is invoked from an assertEventually periodically it is expected that we eventually observe the
        // right values and this doesn't cause false failures due to visibility issues
        int notEmptyPartitions = 0;
        for (HazelcastInstance instance : cluster.getMembers()) {
            NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
            MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
            for (PartitionContainer partitionContainer : service.getMapServiceContext().getPartitionContainers()) {
                int partitionId = partitionContainer.getPartitionId();
                InternalPartitionService partitionService = nodeEngine.getPartitionService();
                InternalPartition partition = partitionService.getPartition(partitionId);
                if (partition.isLocal()) {
                    RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                    if (recordStore.size() != 0) {
                        notEmptyPartitions++;
                    }
                }
            }
        }

        return notEmptyPartitions;
    }

}
