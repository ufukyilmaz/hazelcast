package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.Member;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.WanSyncStateImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.wan.WanSyncStatus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Manages the initiation of WAN sync requests
 */
public class WanSyncManager {

    private static final int RETRY_INTERVAL_MILLIS = 5000;
    private static final AtomicReferenceFieldUpdater<WanSyncManager, WanSyncStatus> SYNC_STATUS
            = newUpdater(WanSyncManager.class, WanSyncStatus.class, "syncStatus");

    private final IPartitionService partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final EnterpriseWanReplicationService wanReplicationService;
    private final NodeEngine nodeEngine;
    private final ILogger logger;

    private volatile WanSyncStatus syncStatus = WanSyncStatus.READY;

    private volatile boolean running = true;

    public WanSyncManager(NodeEngine nodeEngine) {
        partitionService = nodeEngine.getPartitionService();
        clusterService = nodeEngine.getClusterService();
        operationService = nodeEngine.getOperationService();
        wanReplicationService = (EnterpriseWanReplicationService) nodeEngine.getWanReplicationService();
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(getClass());
    }

    public void shutdown() {
        running = false;
    }

    public void initiateSyncRequest(final String wanReplicationName,
                                       final String targetGroupName,
                                       final WanSyncEvent syncEvent) {
        //First check if endpoint exists for the given wanReplicationName and targetGroupName
        wanReplicationService.getEndpoint(wanReplicationName, targetGroupName);
        if (!SYNC_STATUS.compareAndSet(this, WanSyncStatus.READY, WanSyncStatus.IN_PROGRESS)) {
            throw new SyncFailedException("Another sync request is already in progress.");
        }
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

    public WanSyncState getWanSyncState() {
        return new WanSyncStateImpl(SYNC_STATUS.get(this));
    }

    public void populateSyncRequestOnMembers(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        try {
            Set<Member> members = clusterService.getMembers();
            List<Future<WanSyncResult>> futures = new ArrayList<Future<WanSyncResult>>(members.size());
            for (Member member : clusterService.getMembers()) {
                WanSyncEvent wanSyncEvent = new WanSyncEvent(syncEvent.getType(), syncEvent.getName());
                Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, wanSyncEvent);
                Future<WanSyncResult> future = operationService.invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                        operation, member.getAddress());
                futures.add(future);
            }

            Set<Integer> partitionIds = getAllPartitionIds();
            addResultOfOps(futures, partitionIds);

            while (!partitionIds.isEmpty() && running) {
                futures.clear();
                logger.info("WAN sync will retry missing partitions - " + partitionIds);

                for (Member member : clusterService.getMembers()) {
                    WanSyncEvent wanSyncEvent = new WanSyncEvent(syncEvent.getType(), syncEvent.getName());
                    wanSyncEvent.setPartitionSet(partitionIds);
                    Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, wanSyncEvent);
                    Future<WanSyncResult> future = operationService.invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                            operation, member.getAddress());
                    futures.add(future);
                }
                addResultOfOps(futures, partitionIds);
                if (!partitionIds.isEmpty()) {
                    try {
                        Thread.sleep(RETRY_INTERVAL_MILLIS);
                    } catch (InterruptedException ignored) {
                        EmptyStatement.ignore(ignored);
                    }
                }
            }
        } finally {
            SYNC_STATUS.set(this, WanSyncStatus.READY);
        }
    }

    private void addResultOfOps(List<Future<WanSyncResult>> futures, Set<Integer> partitionIds) {
        boolean alreadyLogged = false;
        for (Future<WanSyncResult> future : futures) {
            try {
                WanSyncResult result = future.get();
                partitionIds.removeAll(result.getSyncedPartitions());
            } catch (Exception ex) {
                if (!alreadyLogged) {
                    logger.warning("Exception occurred during WAN sync, missing partitions will be retried.", ex);
                    alreadyLogged = true;
                }
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
