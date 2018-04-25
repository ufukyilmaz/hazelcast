package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.Member;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.WanSyncStateImpl;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.wan.WanSyncStatus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Manages the initiation of WAN sync requests
 */
public class WanSyncManager {
    private static final int RETRY_INTERVAL_MILLIS = 5000;
    private static final int MAX_RETRY_COUNT = 5;
    private static final AtomicReferenceFieldUpdater<WanSyncManager, WanSyncStatus> SYNC_STATUS
            = newUpdater(WanSyncManager.class, WanSyncStatus.class, "syncStatus");
    private static final AtomicIntegerFieldUpdater<WanSyncManager> SYNCED_PARTITION_COUNT
            = AtomicIntegerFieldUpdater.newUpdater(WanSyncManager.class, "syncedPartitionCount");

    private final EnterpriseWanReplicationService wanReplicationService;
    private final ILogger logger;
    private final Node node;

    private volatile WanSyncStatus syncStatus = WanSyncStatus.READY;
    /** The processed {@link WanSyncEvent}s count */
    private volatile int syncedPartitionCount;

    private volatile boolean running = true;
    private volatile String activeWanConfig;
    private volatile String activePublisher;

    public WanSyncManager(EnterpriseWanReplicationService wanReplicationService, Node node) {
        this.node = node;
        this.wanReplicationService = wanReplicationService;
        this.logger = node.getLogger(getClass());
    }

    public void shutdown() {
        running = false;
    }

    public void initiateSyncRequest(final String wanReplicationName,
                                    final String targetGroupName,
                                    final WanSyncEvent syncEvent) {
        // first check if endpoint exists for the given wanReplicationName and targetGroupName
        wanReplicationService.getEndpoint(wanReplicationName, targetGroupName);
        if (!SYNC_STATUS.compareAndSet(this, WanSyncStatus.READY, WanSyncStatus.IN_PROGRESS)) {
            throw new SyncFailedException("Another sync request is already in progress.");
        }
        activeWanConfig = wanReplicationName;
        activePublisher = targetGroupName;
        node.nodeEngine.getExecutionService().execute("hz:wan:sync:pool", new Runnable() {
            @Override
            public void run() {
                Operation operation = new WanSyncStarterOperation(wanReplicationName, targetGroupName, syncEvent);
                getOperationService().invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                        operation, getClusterService().getThisAddress());
            }
        });
        logger.info("WAN sync request has been sent");
    }

    public WanSyncState getWanSyncState() {
        return new WanSyncStateImpl(syncStatus, syncedPartitionCount, activeWanConfig, activePublisher);
    }

    public void populateSyncRequestOnMembers(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        int retryCount = 0;
        try {
            Set<Member> members = getClusterService().getMembers();
            List<Future<WanSyncResult>> futures = new ArrayList<Future<WanSyncResult>>(members.size());
            for (Member member : getClusterService().getMembers()) {
                WanSyncEvent wanSyncEvent = new WanSyncEvent(syncEvent.getType(), syncEvent.getName());
                Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, wanSyncEvent);
                Future<WanSyncResult> future = getOperationService()
                        .invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME, operation, member.getAddress());
                futures.add(future);
            }

            Set<Integer> partitionIds = getAllPartitionIds();
            addResultOfOps(futures, partitionIds);
            while (!partitionIds.isEmpty() && running) {
                futures.clear();
                logger.info("WAN sync will retry missing partitions - " + partitionIds);

                for (Member member : getClusterService().getMembers()) {
                    WanSyncEvent wanSyncEvent = new WanSyncEvent(syncEvent.getType(), syncEvent.getName());
                    wanSyncEvent.setPartitionSet(partitionIds);
                    Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName, wanSyncEvent);
                    Future<WanSyncResult> future = getOperationService()
                            .invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME, operation, member.getAddress());
                    futures.add(future);
                }
                addResultOfOps(futures, partitionIds);
                if (!partitionIds.isEmpty()) {
                    if (++retryCount == MAX_RETRY_COUNT) {
                        logger.warning("Quitting retrying to sync missing partitions. WAN Sync has failed.");
                        break;
                    }
                    try {
                        Thread.sleep(RETRY_INTERVAL_MILLIS);
                    } catch (InterruptedException ignored) {
                        currentThread().interrupt();
                    }
                }
            }
        } finally {
            SYNC_STATUS.set(this, retryCount == MAX_RETRY_COUNT ? WanSyncStatus.FAILED : WanSyncStatus.READY);
        }
    }

    private InternalOperationService getOperationService() {
        return node.nodeEngine.getOperationService();
    }

    public void incrementSyncedPartitionCount() {
        SYNCED_PARTITION_COUNT.incrementAndGet(this);
    }

    public void resetSyncedPartitionCount() {
        SYNCED_PARTITION_COUNT.set(this, 0);
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
        int partitionCount = getPartitionService().getPartitionCount();
        return createSetWithPopulatedPartitionIds(partitionCount);
    }

    private Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
        Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }

    /**
     * Returns this node's partition service
     */
    private IPartitionService getPartitionService() {
        return node.getPartitionService();
    }

    /**
     * Returns this node's cluster service
     */
    private ClusterService getClusterService() {
        return node.getClusterService();
    }
}
