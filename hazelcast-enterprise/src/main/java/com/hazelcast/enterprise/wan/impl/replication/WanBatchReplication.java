package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.enterprise.wan.impl.AbstractWanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventResult;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanAntiEntropyEvent;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;

/**
 * WAN replication publisher that sends events in batches.
 * Basically, it publishes events either when enough events are enqueued
 * or enqueued events have waited for enough time.
 * <p>
 * The event count is configurable with
 * {@link WanBatchReplicationPublisherConfig#setBatchSize(int)}.
 * The elapsed time is configurable by
 * {@link WanBatchReplicationPublisherConfig#setBatchMaxDelayMillis(int)}.
 * The events are sent to the endpoints depending on the event key
 * partition.
 *
 * <b>NOTE</b>
 * The WAN batch collection mechanism needs to be run by one thread only
 * for a single publisher.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class WanBatchReplication extends AbstractWanReplication implements Runnable, LiveOperationsTracker {
    /**
     * JVM argument for changing the implementation of the base implementation
     * for the {@link WanBatchSender}.
     * Intended for testing and benchmarking, e.g. where the target cluster can
     * be mocked by generating responses after a configurable latency.
     */
    public static final String WAN_BATCH_SENDER_CLASS = "hazelcast.wan.wanBatchSenderClass";

    /**
     * Executor name for WAN replication tasks, e.g. collecting batches and
     * sending them to target cluster, handling responses, performing WAN sync
     * and merkle tree comparisons, etc.
     * The default will use the cached executor but users can define a concrete
     * executor if WAN replication requires separate threads.
     */
    public static final String WAN_EXECUTOR = "hz:wan";

    private static final int IDLE_MAX_SPINS = 20;
    private static final int IDLE_MAX_YIELDS = 50;

    protected WanBatchSender wanBatchSender;

    private final AtomicLong failedTransmitCount = new AtomicLong();
    private final Set<Operation> liveOperations = newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Lock for coordinating WAN sync process with dequeueing WAN events from
     * the WAN queues. This lock should prevent adverse effects such as a sync
     * event being enqueued after which a remove event is dequeued for that
     * entry from the WAN queues. If the remove event was replicated to the
     * target cluster first, after which the sync event was replicated, the
     * entry would be resurrected.
     * For the duration of WAN sync, we don't allow dequeueing WAN events from
     * the WAN partition queues.
     */
    private final ReentrantLock syncLock = new ReentrantLock();
    private AtomicLong ongoingSyncInvocations = new AtomicLong();

    private volatile long lastBatchSendTime = System.currentTimeMillis();

    private Executor wanExecutor;
    private BlockingQueue<InternalWanReplicationEvent> syncEvents;
    private IdleStrategy idlingStrategy;
    private ArrayList<InternalWanReplicationEvent> eventBatchHolder;

    private BatchReplicationStrategy replicationStrategy;

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig wanPublisherConfig) {
        super.init(wanReplicationConfig, wanPublisherConfig);
        this.idlingStrategy = new BackoffIdleStrategy(
                IDLE_MAX_SPINS,
                IDLE_MAX_YIELDS,
                configurationContext.getIdleMinParkNs(),
                configurationContext.getIdleMaxParkNs());
        this.wanExecutor = node.getNodeEngine().getExecutionService().getExecutor(WAN_EXECUTOR);
        this.wanBatchSender = createWanBatchSender(node);
        this.syncEvents = new LinkedBlockingQueue<>(configurationContext.getBatchSize());
        this.eventBatchHolder = new ArrayList<>(configurationContext.getBatchSize());

        int maxConcurrentInvocations = configurationContext.getMaxConcurrentInvocations();
        logger.fine("Initialising WAN batch publisher with " + maxConcurrentInvocations + " max invocations.");
        if (maxConcurrentInvocations > 1) {
            replicationStrategy = new ConcurrentBatchReplicationStrategy(maxConcurrentInvocations);
        } else {
            replicationStrategy = new SerialBatchReplicationStrategy();
        }

        // the execution of this WAN replication needs to be restricted to a single thread
        wanExecutor.execute(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method mustn't run concurrently.
     */
    @Override
    public void run() {
        int idleCount = 0;
        while (running) {
            try {
                if (tryMakeProgress()) {
                    idleCount = 0;
                } else {
                    idlingStrategy.idle(idleCount++);
                }
            } catch (Exception e) {
                logger.severe("Exception occurred in WAN replication loop", e);
            }
        }
    }

    /**
     * Attempts to make progress with WAN replication, i.e. it will try to find
     * any target endpoints, collect any WAN sync events or regular WAN events,
     * create a batch and send them to the target cluster.
     * If this method actually does any progress, it will return {@code true}.
     * Otherwise, if no progress was possible (e.g. no target endpoints were
     * available) or there were no events, it will return {@code false}.
     *
     * @return {@code true} if this method made any progress, {@code false} otherwise
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean tryMakeProgress() {
        List<Address> endpoints = getTargetEndpoints();
        if (endpoints.isEmpty()) {
            return false;
        }

        Address endpoint = replicationStrategy.getNextEventBatchEndpoint(endpoints);
        if (endpoint == null) {
            return false;
        }

        if (!syncEvents.isEmpty()) {
            BatchWanReplicationEvent batch = new BatchWanReplicationEvent(configurationContext.isSnapshotEnabled());
            ArrayList<InternalWanReplicationEvent> batchList = new ArrayList<>(configurationContext.getBatchSize());
            syncEvents.drainTo(batchList, configurationContext.getBatchSize());
            for (InternalWanReplicationEvent event : batchList) {
                batch.addEvent(event);
            }
            boolean sent = sendBatch(endpoint, batch, true);
            if (sent) {
                ongoingSyncInvocations.incrementAndGet();
            }
            return true;
        }

        if (state.isReplicateEnqueuedEvents()) {
            if (syncLock.tryLock()) {
                BatchWanReplicationEvent batch = null;
                try {
                    if (!hasOngoingSync()) {
                        batch = collectEventBatch(endpoint, endpoints);
                    }
                } finally {
                    syncLock.unlock();
                }
                if (batch != null) {
                    sendBatch(endpoint, batch, false);
                    return true;
                }
            }
        }

        replicationStrategy.complete(endpoint);
        return false;
    }

    /**
     * Returns {@code true} if there are any pending WAN sync events to be
     * replicated or a batch with WAN sync events is currently being replicated
     * to the target cluster. This method must be called while holding the
     * {@link #syncLock}.
     *
     * @return {@code true} if there is an ongoing WAN sync
     */
    private boolean hasOngoingSync() {
        assert syncLock.isHeldByCurrentThread();
        return !syncEvents.isEmpty() || ongoingSyncInvocations.get() > 0;
    }

    /**
     * Tries to collect a batch of WAN replication events for the given
     * {@code endpoint} in accordance to the configured replication strategy.
     * The method will try and collect a batch of events until the configured
     * batch size is reached or enough time has passed since the last WAN batch
     * was sent to any target endpoint.
     * If no batch has been collected, it will return {@code null}.
     *
     * @param endpoint  the endpoint to which WAN events should be sent
     * @param endpoints the complete list of target endpoints
     * @return the collected WAN batch or {@code null} if nothing was collected
     * @see WanBatchReplicationPublisherConfig#getBatchSize()
     * @see WanBatchReplicationPublisherConfig#getBatchMaxDelayMillis()
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private BatchWanReplicationEvent collectEventBatch(Address endpoint, List<Address> endpoints) {
        InternalPartitionService partitionService = node.getPartitionService();
        BatchWanReplicationEvent batch = null;

        boolean collectionPeriodPassed = false;
        boolean collectedAnyEvent;
        do {
            collectedAnyEvent = false;
            for (int partitionId = replicationStrategy.getFirstPartitionId(endpoint, endpoints);
                 partitionId < partitionService.getPartitionCount() && !collectionPeriodPassed;
                 partitionId += replicationStrategy.getPartitionIdStep(endpoint, endpoints)) {

                if (!partitionService.getPartition(partitionId).isLocal()) {
                    continue;
                }

                int elementsToDrain = configurationContext.getBatchSize() - (batch == null ? 0 : batch.getTotalEntryCount());
                eventBatchHolder.clear();
                eventQueueContainer.drainRandomWanQueue(partitionId, eventBatchHolder, elementsToDrain);

                for (InternalWanReplicationEvent event : eventBatchHolder) {
                    if (batch == null) {
                        batch = new BatchWanReplicationEvent(
                                configurationContext.isSnapshotEnabled());
                    }
                    batch.addEvent(event);
                    collectedAnyEvent = true;
                }

                int entryCount = batch == null ? 0 : batch.getTotalEntryCount();

                collectionPeriodPassed = entryCount >= configurationContext.getBatchSize()
                        || (sendingPeriodPassed() && entryCount > 0)
                        || !running;
            }
        } while (!(collectionPeriodPassed || sendingPeriodPassed()) && collectedAnyEvent);

        return batch;
    }

    /**
     * Checks if {@link WanBatchReplicationPublisherConfig#getBatchMaxDelayMillis()}
     * has passed since the last replication was sent
     */
    private boolean sendingPeriodPassed() {
        long elapsedMillis = System.currentTimeMillis() - lastBatchSendTime;
        long maxDelayMillis = configurationContext.getBatchMaxDelayMillis();
        return elapsedMillis > maxDelayMillis;
    }

    private boolean sendBatch(final Address endpoint,
                              final BatchWanReplicationEvent batch,
                              final boolean isSyncBatch) {
        if (!running) {
            return false;
        }

        try {
            InternalCompletableFuture<Boolean> future = wanBatchSender.send(batch, endpoint);
            future.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    handleWanBatchResponse(batch, endpoint, response, isSyncBatch);
                } else {
                    handleWanBatchError(batch, endpoint, t, isSyncBatch);
                }
            }, wanExecutor);
            lastBatchSendTime = System.currentTimeMillis();
        } catch (Throwable t) {
            handleWanBatchError(batch, endpoint, t, isSyncBatch);
        }
        return true;
    }


    /**
     * Handles a response for a transmitted WAN batch.
     *
     * @param batch    the WAN batch
     * @param endpoint the endpoint to which the batch was sent
     * @param response the endpoint response
     */
    private void handleWanBatchResponse(BatchWanReplicationEvent batch,
                                        Address endpoint,
                                        boolean response,
                                        boolean isSyncBatch) {
        if (response) {
            for (InternalWanReplicationEvent sentEvent : batch.getEvents()) {
                incrementEventCount(sentEvent);
            }

            finalizeWanEventReplication(batch.getEvents(), batch.getCoalescedEvents());
            replicationStrategy.complete(endpoint);
            wanCounter.decrementPrimaryElementCounter(batch.getPrimaryEventCount());
            if (isSyncBatch) {
                ongoingSyncInvocations.decrementAndGet();
            }
        } else {
            failedTransmitCount.incrementAndGet();
            sendBatch(endpoint, batch, isSyncBatch);
        }
    }

    /**
     * Handles an error that occurred when sending a WAN batch.
     *
     * @param batch    the WAN batch
     * @param endpoint the endpoint to which the batch was sent
     * @param error    the error which occurred
     */
    private void handleWanBatchError(BatchWanReplicationEvent batch,
                                     Address endpoint,
                                     Throwable error,
                                     boolean isSyncBatch) {
        logger.warning("Error occurred when sending WAN events to " + endpoint, error);
        connectionManager.removeTargetEndpoint(endpoint,
                "Error occurred when sending WAN events to " + endpoint, error);
        failedTransmitCount.incrementAndGet();
        sendBatch(endpoint, batch, isSyncBatch);
    }

    @Override
    protected WanPublisherSyncSupport createWanSyncSupport() {
        WanSyncConfig syncConfig = configurationContext.getPublisherConfig().getWanSyncConfig();
        if (syncConfig != null && MERKLE_TREES.equals(syncConfig.getConsistencyCheckStrategy())) {
            return new WanPublisherMerkleTreeSyncSupport(node, configurationContext, this);
        } else {
            return new WanPublisherFullSyncSupport(node, this);
        }
    }

    /**
     * Creates and returns the {@link WanBatchSender} based on the configuration.
     *
     * @param node this members {@link Node}
     * @return the WAN batch sender
     */
    private WanBatchSender createWanBatchSender(Node node) {
        WanBatchSender sender = createBaseWanBatchSender(node);
        sender.init(node, this);

        Diagnostics diagnostics = node.getNodeEngine().getDiagnostics();
        StoreLatencyPlugin storeLatencyPlugin = diagnostics.getPlugin(StoreLatencyPlugin.class);
        return storeLatencyPlugin != null
                ? new LatencyTrackingWanBatchSender(sender, storeLatencyPlugin, wanPublisherId, wanExecutor)
                : sender;
    }

    private WanBatchSender createBaseWanBatchSender(Node node) {
        String senderClass = System.getProperty(WAN_BATCH_SENDER_CLASS);
        if (isNullOrEmpty(senderClass)) {
            return new DefaultWanBatchSender();
        }
        try {
            return ClassLoaderUtil.newInstance(node.getConfigClassLoader(), senderClass);
        } catch (Exception e) {
            throw new HazelcastException("Could not construct WAN batch sender", e);
        }
    }

    /**
     * Publishes an anti-entropy event.
     * This method does not wait for the event processing to complete.
     *
     * @param wanAntiEntropyEvent the WAN anti-entropy event
     */
    @Override
    public void publishAntiEntropyEvent(WanAntiEntropyEvent wanAntiEntropyEvent) {
        AbstractWanAntiEntropyEvent event = (AbstractWanAntiEntropyEvent) wanAntiEntropyEvent;
        liveOperations.add(event.getOp());

        wanExecutor.execute(() -> {
            event.setProcessingResult(new WanAntiEntropyEventResult());
            try {
                if (event instanceof WanSyncEvent) {
                    syncLock.lock();
                    try {
                        syncSupport.processEvent((WanSyncEvent) event);
                    } finally {
                        syncLock.unlock();
                    }
                    return;
                }
                if (event instanceof WanConsistencyCheckEvent) {
                    syncSupport.processEvent((WanConsistencyCheckEvent) event);
                    return;
                }

                logger.info("Ignoring unknown WAN anti-entropy event " + event);
            } catch (Exception ex) {
                logger.warning("WAN anti-entropy event " + event + " processing failed", ex);
            } finally {
                event.sendResponse();
                liveOperations.remove(event.getOp());
            }
        });
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (Operation op : this.liveOperations) {
            liveOperations.add(op.getCallerAddress(), op.getCallId());
        }
    }

    /**
     * Returns the number of failed WAN batch transmissions.
     */
    public long getFailedTransmissionCount() {
        return failedTransmitCount.get();
    }

    /**
     * Returns the executor used for WAN replication.
     */
    Executor getWanExecutor() {
        return wanExecutor;
    }

    /**
     * Puts the provided event to the queue for WAN sync events, waiting if
     * necessary for space to become available.
     *
     * @param event the WAN event
     */
    void putToSyncEventQueue(InternalWanReplicationEvent event) {
        try {
            syncEvents.put(event);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw rethrow(e);
        }
    }

    // for testing
    public BatchReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }
}
