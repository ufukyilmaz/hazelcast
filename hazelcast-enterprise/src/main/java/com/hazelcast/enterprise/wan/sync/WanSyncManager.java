package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Manages the initiation of WAN sync requests
 */
public class WanSyncManager {

    private final IPartitionService partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final ILogger logger;

    public WanSyncManager(NodeEngine nodeEngine) {
        partitionService = nodeEngine.getPartitionService();
        clusterService = nodeEngine.getClusterService();
        operationService = nodeEngine.getOperationService();
        logger = nodeEngine.getLogger(getClass());
    }

    public SyncResult initiateSyncOnAllPartitions(String wanReplicationName,
                                                  String targetGroupName,
                                                  String mapName) {
        Set<Integer> partitionIds = getAllPartitionIds();
        SyncResult result = new SyncResult();

        List<Future<SyncResult>> futures = startSyncOnMembers(wanReplicationName, targetGroupName, mapName);
        addResultsOfOps(futures, result, partitionIds);

        if (partitionIds.isEmpty()) {
            return result;
        }

        futures = syncMissingPartitions(wanReplicationName, targetGroupName, mapName, partitionIds);
        addResultsOfOps(futures, result, partitionIds);

        return result;
    }

    private void addResultsOfOps(List<Future<SyncResult>> futures, SyncResult result,  Set<Integer> partitionIds) {
        try {
            for (Future<SyncResult> future : futures) {
                SyncResult syncResult = future.get();
                Set<Integer> syncInitiatedPartitions = syncResult.getPartitionIds();
                if (syncInitiatedPartitions != null) {
                    partitionIds.removeAll(syncInitiatedPartitions);
                    result.getPartitionIds().addAll(syncInitiatedPartitions);
                }
            }
        } catch (Exception ex) {
            logger.severe("Exception occurred while WAN sync initiation", ex);
        }
    }

    private List<Future<SyncResult>> startSyncOnMembers(String wanReplicationName,
                                                        String targetGroupName,
                                                        String mapName) {
        Set<Member> members = clusterService.getMembers();
        List<Future<SyncResult>> futures = new ArrayList<Future<SyncResult>>(members.size());
        for (Member member : members) {
            Operation operation = new MemberMapSyncOperation(wanReplicationName, targetGroupName, mapName);
            Future<SyncResult> future = operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    private List<Future<SyncResult>> syncMissingPartitions(String wanReplicationName,
                                                           String targetGroupName,
                                                           String mapName, Set<Integer> partitionIds) {
        List<Future<SyncResult>> futures = new ArrayList<Future<SyncResult>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            Operation op = new PartitionMapSyncOperation(wanReplicationName, targetGroupName, mapName);
            op.setPartitionId(partitionId);
            Future<SyncResult> future = operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            futures.add(future);
        }
        return futures;
    }

    public Set<Integer> getAllPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        return createSetWithPopulatedPartitionIds(partitionCount);
    }

    private Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
        Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }

}
