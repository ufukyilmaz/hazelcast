package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.wan.impl.AbstractWanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.internal.util.collection.InflatableSet.Builder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.WanSyncStateImpl;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.wan.impl.WanSyncStatus;

import java.util.ArrayList;
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

    /**
     * Initiates a WAN anti-entropy event and designates this member as the
     * coordinator to broadcast the event to other cluster members.
     * This method will return as soon as the anti-entropy publication has
     * initiated but not yet processed.
     *
     * @param wanReplicationName name of WAN replication configuration (scheme)
     * @param wanPublisherId     WAN replication publisher ID
     * @param event              the WAN anti-entropy event
     * @throws SyncFailedException if there is an ongoing anti-entropy event being processed
     */
    public void initiateAntiEntropyRequest(String wanReplicationName,
                                           String wanPublisherId,
                                           AbstractWanAntiEntropyEvent event) {
        // first check if publisher exists for the given wanReplicationName and wanPublisherId
        wanReplicationService.getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (!SYNC_STATUS.compareAndSet(this, WanSyncStatus.READY, WanSyncStatus.IN_PROGRESS)) {
            throw new SyncFailedException("Another anti-entropy request is already in progress.");
        }
        activeWanConfig = wanReplicationName;
        activePublisher = wanPublisherId;

        node.getNodeEngine().getExecutionService().execute("hz:wan:sync:pool", () -> {
            Operation op = new WanAntiEntropyEventStarterOperation(wanReplicationName, wanPublisherId, event);
            InternalCompletableFuture<Object> future = getOperationService()
                    .invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME, op, node.getThisAddress());
            future.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    logger.info("WAN anti-entropy request " + event + " has been processed");
                } else {
                    logger.warning("WAN anti-entropy request " + event + " processing failed", t);
                }
            });
        });
        logger.info("WAN anti-entropy request " + event + " has been sent");
    }

    public WanSyncState getWanSyncState() {
        return new WanSyncStateImpl(syncStatus, syncedPartitionCount, activeWanConfig, activePublisher);
    }

    /**
     * Keeps broadcasting the WAN anti-entropy event to all members until it
     * has been triggered for all partitions or some partitions have failed for
     * {@value MAX_RETRY_COUNT} times.
     * <p>
     * This method merely is concerned with publishing the event on all
     * partitions. Whether the event is fully processed when this method returns
     * depends on the semantics of processing each event type.
     * In case of WAN sync event, the sync is not complete when this method
     * returns. After this method returns, entries for all partitions have been
     * enqueued but not yet replicated.
     *
     * @param wanReplicationName name of WAN replication configuration (scheme)
     * @param wanPublisherId     WAN replication publisher ID
     * @param event              the WAN anti-entropy event
     */
    void publishAntiEntropyEventOnMembers(String wanReplicationName,
                                          String wanPublisherId,
                                          AbstractWanAntiEntropyEvent event) {
        int retryCount = 0;
        try {
            Set<Integer> partitionsToSync = event.getPartitionSet();

            while (running) {
                broadcastEvent(wanReplicationName, wanPublisherId, event, partitionsToSync);

                if (partitionsToSync.isEmpty()) {
                    break;
                }

                if (++retryCount == MAX_RETRY_COUNT) {
                    logger.warning(String.format("WAN anti-entropy event publication failed after %s attempts"
                            + " with %s partitions not processed", MAX_RETRY_COUNT, partitionsToSync.size()));
                    break;
                }
                logger.info(String.format("WAN anti-entropy event publication will retry "
                        + "because %s partitions have not been processed", partitionsToSync.size()));

                try {
                    Thread.sleep(RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                }
            }
        } finally {
            SYNC_STATUS.set(this, retryCount == MAX_RETRY_COUNT ? WanSyncStatus.FAILED : WanSyncStatus.READY);
        }
    }

    /**
     * Broadcasts the {@code event} to all cluster members.
     * The {@code partitionsToSync} map provides the keys for which the event has
     * not yet been processed. Entries will be removed from this map once the
     * event has been successfully processed. This method returns the map after
     * a single round of broadcast is performed.
     * <p>
     * This method returns as soon as all cluster invocations return, either
     * successfully or with a failure.
     * This method merely is concerned with publishing the event on all
     * partitions. Whether the event is fully processed when this method returns
     * depends on the semantics of processing each event type.
     * In case of WAN sync event, the sync is not complete when this method
     * returns. After this method returns, entries for all partitions have been
     * enqueued but not yet replicated.
     *
     * @param wanReplicationName name of WAN replication configuration (scheme)
     * @param wanPublisherId     WAN replication publisher ID
     * @param event              the WAN anti-entropy event
     * @param partitionsToSync   keys for which this this event applies to,
     */
    private void broadcastEvent(String wanReplicationName,
                                String wanPublisherId,
                                AbstractWanAntiEntropyEvent event,
                                Set<Integer> partitionsToSync) {
        final Set<Member> members = getClusterService().getMembers();
        final List<Future<WanAntiEntropyEventResult>> futures = new ArrayList<>(members.size());

        for (Member member : members) {
            AbstractWanAntiEntropyEvent clonedEvent = event.cloneWithoutPartitionKeys();
            clonedEvent.setPartitionSet(partitionsToSync);

            Operation operation = new WanAntiEntropyEventPublishOperation(wanReplicationName, wanPublisherId, clonedEvent);
            Future<WanAntiEntropyEventResult> future = getOperationService()
                    .invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        if (partitionsToSync == null) {
            partitionsToSync = getAllPartitions();
        }
        addResultOfOps(futures, partitionsToSync);
    }

    private OperationService getOperationService() {
        return node.getNodeEngine().getOperationService();
    }

    public void incrementSyncedPartitionCount() {
        SYNCED_PARTITION_COUNT.incrementAndGet(this);
    }

    public void resetSyncedPartitionCount() {
        SYNCED_PARTITION_COUNT.set(this, 0);
    }

    /**
     * Removes partitions for which WAN sync has been successfully triggered
     * from the {@code partitionsToSync} set.
     *
     * @param futures          the  list of futures representing pending completion
     *                         of the WAN sync trigger task
     * @param partitionsToSync IDs of remaining partitions to be synced
     */
    private void addResultOfOps(List<Future<WanAntiEntropyEventResult>> futures,
                                Set<Integer> partitionsToSync) {
        boolean alreadyLogged = false;
        for (Future<WanAntiEntropyEventResult> future : futures) {
            try {
                WanAntiEntropyEventResult result = future.get();
                partitionsToSync.removeAll(result.getProcessedPartitions());
            } catch (Exception ex) {
                if (!alreadyLogged) {
                    logger.warning("Exception occurred during WAN sync, missing WAN sync objects will be retried.", ex);
                    alreadyLogged = true;
                }
            }
        }
    }

    private Set<Integer> getAllPartitions() {
        int partitionCount = getPartitionService().getPartitionCount();
        Builder<Integer> builder = InflatableSet.newBuilder(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            builder.add(i);
        }
        return builder.build();
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
