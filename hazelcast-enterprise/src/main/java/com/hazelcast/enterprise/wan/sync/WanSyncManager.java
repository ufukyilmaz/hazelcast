package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.Member;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
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
    private final NodeEngine nodeEngine;
    private final ILogger logger;


    public WanSyncManager(NodeEngine nodeEngine) {
        partitionService = nodeEngine.getPartitionService();
        clusterService = nodeEngine.getClusterService();
        operationService = nodeEngine.getOperationService();
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(getClass());
    }

    public void initiateSyncRequest(final String wanReplicationName,
                                       final String targetGroupName,
                                       final WanSyncEvent syncEvent) {
        nodeEngine.getExecutionService().execute("hz:wan:sync:pool", new Runnable() {
            @Override
            public void run() {
                Operation operation = new WanSyncStarterOperation(wanReplicationName, targetGroupName, syncEvent);
                operationService.invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                        operation, clusterService.getThisAddress());
            }
        });
        logger.info("WAN sync request has been sent");
    }

    public void populateSyncRequestOnMembers(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        Set<Member> members = clusterService.getMembers();
        List<Future<WanSyncResult>> futures = new ArrayList<Future<WanSyncResult>>(members.size());
        for (Member member : clusterService.getMembers()) {
            Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, syncEvent);
            Future<WanSyncResult> future = operationService.invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                    operation, member.getAddress());
            futures.add(future);
        }

        Set<Integer> partitionIds = getAllPartitionIds();
        addResultOfOps(futures, partitionIds);

        while (!partitionIds.isEmpty()) {
            futures.clear();
            for (Integer partitionId : partitionIds) {
                logger.info("Retrying to sync missing partition - " + partitionId);
                syncEvent.setPartitionId(partitionId);
                Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, syncEvent);
                operation.setPartitionId(partitionId);
                Future<WanSyncResult> future = operationService.invokeOnPartition(EnterpriseWanReplicationService.SERVICE_NAME,
                        operation, partitionId);
                futures.add(future);
            }
            addResultOfOps(futures, partitionIds);
        }
    }

    private void addResultOfOps(List<Future<WanSyncResult>> futures, Set<Integer> partitionIds) {
        for (Future<WanSyncResult> future : futures) {
            try {
                WanSyncResult result = future.get();
                partitionIds.removeAll(result.getSyncedPartitions());
            } catch (Exception ex) {
                logger.warning("Exception occurred during WAN sync, missing partitions will be retried.", ex);
            }
        }
    }

    private Set<Integer> getAllPartitionIds() {
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
