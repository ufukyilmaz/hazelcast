package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.Thread.currentThread;

/**
 * WAN replication publisher that sends events in batches.
 * Basically, it publishes events either when enough events are enqueued
 * or enqueued events have waited for enough time.
 * <p>
 * The event count is configurable by
 * {@link WanReplicationProperties#BATCH_SIZE} and is
 * {@value WanConfigurationContext#DEFAULT_BATCH_SIZE} by default.
 * The elapsed time is configurable by
 * {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} and is
 * {@value WanConfigurationContext#DEFAULT_BATCH_MAX_DELAY_MILLIS} by default.
 * The events are sent to the endpoints depending on the event key
 * partition.
 */
public class WanBatchReplication extends AbstractWanReplication implements Runnable {
    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private final Object mutex = new Object();
    private final AtomicLong failedTransmitCount = new AtomicLong();

    private volatile StripedExecutor executor;

    private volatile long lastBatchSendTime = System.currentTimeMillis();
    private WanBatchSender wanBatchSender;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
        super.init(node, wanReplicationConfig, wanPublisherConfig);
        this.wanBatchSender = createWanBatchSender(node, configurationContext);
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    @Override
    protected void afterShutdown() {
        super.afterShutdown();
        StripedExecutor ex = executor;
        if (ex != null) {
            ex.shutdown();
        }
    }

    @Override
    public int getStagingQueueSize() {
        return configurationContext.getBatchSize();
    }

    @Override
    public void run() {
        while (running) {
            final Map<Address, BatchWanReplicationEvent> batchReplicationEventMap
                    = new ConcurrentHashMap<Address, BatchWanReplicationEvent>();
            final List<WanReplicationEvent> events = drainStagingQueue();
            final List<Address> liveEndpoints = connectionManager.awaitAndGetTargetEndpoints();
            if (liveEndpoints.isEmpty()) {
                continue;
            }

            for (Address target : liveEndpoints) {
                batchReplicationEventMap.put(target,
                        new BatchWanReplicationEvent(configurationContext.isSnapshotEnabled()));
            }

            if (!events.isEmpty()) {
                checkExecutorInitialized(liveEndpoints.size());
                for (WanReplicationEvent event : events) {
                    final int partitionId = getPartitionId(event.getEventObject().getKey());
                    final Address target = liveEndpoints.get(partitionId % liveEndpoints.size());
                    batchReplicationEventMap.get(target).addEvent(event);
                }
                for (Entry<Address, BatchWanReplicationEvent> entry : batchReplicationEventMap.entrySet()) {
                    BatchWanReplicationEvent event = entry.getValue();
                    if (event.getEvents().size() > 0) {
                        handleBatchReplicationEventObject(entry.getKey(), entry.getValue());
                    }
                }
                lastBatchSendTime = System.currentTimeMillis();
            }
        }
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
     * @param node                 this members {@link Node}
     * @param configurationContext the configuration context for this publisher
     * @return the WAN batch sender
     */
    private WanBatchSender createWanBatchSender(Node node, WanConfigurationContext configurationContext) {
        final SerializationService serializationService = node.nodeEngine.getSerializationService();
        final InternalOperationService operationService = node.nodeEngine.getOperationService();
        final DefaultWanBatchSender sender = new DefaultWanBatchSender(
                connectionManager, logger, serializationService, operationService, configurationContext);
        final Diagnostics diagnostics = node.nodeEngine.getDiagnostics();
        final StoreLatencyPlugin storeLatencyPlugin = diagnostics.getPlugin(StoreLatencyPlugin.class);
        return storeLatencyPlugin != null
                ? new LatencyTrackingWanBatchSender(sender, storeLatencyPlugin, wanPublisherId)
                : sender;
    }

    /** Drains the staging queue until enough items have been drained or enough time has passed */
    private List<WanReplicationEvent> drainStagingQueue() {
        int entriesInBatch = 0;
        List<WanReplicationEvent> wanReplicationEventList = new ArrayList<WanReplicationEvent>();
        while (!(entriesInBatch >= configurationContext.getBatchSize()
                || sendingPeriodPassed(wanReplicationEventList.size()))) {
            if (!running) {
                break;
            }
            WanReplicationEvent event = null;
            try {
                event = stagingQueue.poll(configurationContext.getBatchMaxDelayMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
            if (event != null) {
                wanReplicationEventList.add(event);

                if (event.getEventObject() instanceof EnterpriseMapReplicationMerkleTreeNode) {
                    EnterpriseMapReplicationMerkleTreeNode node =
                            (EnterpriseMapReplicationMerkleTreeNode) event.getEventObject();
                    entriesInBatch += node.getEntryCount();
                } else {
                    entriesInBatch++;
                }
            }
        }
        return wanReplicationEventList;
    }

    /**
     * Submits a batch replication runnable to the striped executor for a specific endpoint and retries if the executor
     * rejected the task.
     *
     * @param target                the endpoint for the event
     * @param batchReplicationEvent the batch event
     */
    private void handleBatchReplicationEventObject(final Address target, final BatchWanReplicationEvent batchReplicationEvent) {
        final BatchStripedRunnable batchStripedRunnable = new BatchStripedRunnable(target, batchReplicationEvent);
        boolean taskSubmitted = false;
        do {
            try {
                executor.execute(batchStripedRunnable);
                taskSubmitted = true;
            } catch (RejectedExecutionException ree) {
                logger.fine("WanBatchReplication striped runnable job queue is full. Retrying.");
            }
        } while (!taskSubmitted && running);
    }

    /** Checks if {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} has passed since the last replication was sent */
    private boolean sendingPeriodPassed(int eventQueueSize) {
        return System.currentTimeMillis() - lastBatchSendTime > configurationContext.getBatchMaxDelayMillis()
                && eventQueueSize > 0;
    }

    /**
     * Initializes the striped executor if necessary. If {@link WanReplicationProperties#EXECUTOR_THREAD_COUNT} is defined, it
     * will use that number of threads, otherwise it will use the {@code threadCount}.
     */
    private void checkExecutorInitialized(int threadCount) {
        if (executor == null) {
            synchronized (mutex) {
                if (executor == null) {
                    final int overridenThreadCount = configurationContext.getExecutorThreadCount() > 0
                            ? configurationContext.getExecutorThreadCount()
                            : threadCount;
                    executor = new StripedExecutor(node.getLogger(WanBatchReplication.class),
                            createThreadName(node.hazelcastInstance.getName(), "wan-batch-replication"),
                            overridenThreadCount,
                            STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
            }
        }
    }

    /**
     * Returns the number of failed WAN batch transmissions.
     */
    public long getFailedTransmissionCount() {
        return failedTransmitCount.get();
    }

    /**
     * {@link StripedRunnable} implementation to send Batch of WAN replication events to target cluster. It will retry
     * sending the event until it has succeeded or until stopped. The WAN event backups are removed from the replicas
     * after the batch has been sent and operation response received from the target cluster.
     */
    private class BatchStripedRunnable implements StripedRunnable, TimeoutRunnable {
        private final Address target;
        private final BatchWanReplicationEvent batchReplicationEvent;

        public BatchStripedRunnable(Address target, BatchWanReplicationEvent batchReplicationEvent) {
            this.target = target;
            this.batchReplicationEvent = batchReplicationEvent;
        }

        @Override
        public void run() {
            boolean transmissionSucceeded;
            do {
                transmissionSucceeded = wanBatchSender.send(batchReplicationEvent, target);
                if (transmissionSucceeded) {
                    for (WanReplicationEvent sentEvent : batchReplicationEvent.getEvents()) {
                        incrementEventCount(sentEvent);
                        removeReplicationEvent(sentEvent);

                        // removing the coalesced events
                        Queue<WanReplicationEvent> coalescedEvents = batchReplicationEvent.getCoalescedEvents(sentEvent);
                        for (WanReplicationEvent coalescedEvent : coalescedEvents) {
                            removeReplicationEvent(coalescedEvent);
                        }
                    }
                    wanCounter.decrementPrimaryElementCounter(batchReplicationEvent.getAddedEventCount());
                } else {
                    failedTransmitCount.incrementAndGet();
                }
            } while (!transmissionSucceeded && running);
        }

        @Override
        public int getKey() {
            return target.hashCode();
        }

        @Override
        public long getTimeout() {
            return STRIPED_RUNNABLE_TIMEOUT_SECONDS;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.SECONDS;
        }
    }
}
