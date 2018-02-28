package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_SIZE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.EXECUTOR_THREAD_COUNT;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.SNAPSHOT_ENABLED;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.Thread.currentThread;

/**
 * WAN replication publisher that sends events in batches.
 * Basically, it publishes events either when enough events are enqueued or enqueued events have waited for enough time.
 * <p>
 * The event count is configurable by {@link WanReplicationProperties#BATCH_SIZE} and is {@value DEFAULT_BATCH_SIZE} by default.
 * The elapsed time is configurable by {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} and is
 * {@value DEFAULT_BATCH_MAX_DELAY_MILLIS} by default.
 * The events are sent to the endpoints depending on the event key partition.
 */
public class WanBatchReplication extends AbstractWanReplication implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private final Object mutex = new Object();
    private final AtomicLong failureCount = new AtomicLong();

    private volatile StripedExecutor executor;

    private volatile long lastBatchSendTime = System.currentTimeMillis();
    private boolean snapshotEnabled;
    private int executorThreadCount;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
        super.init(node, wanReplicationConfig, wanPublisherConfig);
        this.snapshotEnabled = getProperty(SNAPSHOT_ENABLED, wanPublisherConfig.getProperties(), false);
        this.executorThreadCount = getProperty(EXECUTOR_THREAD_COUNT, wanPublisherConfig.getProperties(), -1);

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
        return getProperty(BATCH_SIZE, publisherConfig.getProperties(), DEFAULT_BATCH_SIZE);
    }

    @Override
    public void run() {
        while (running) {
            final Map<Address, BatchWanReplicationEvent> batchReplicationEventMap
                    = new ConcurrentHashMap<Address, BatchWanReplicationEvent>();
            final List<WanReplicationEvent> events = drainStagingQueue();
            final List<Address> liveEndpoints = awaitAndGetTargetEndpoints();
            if (liveEndpoints.isEmpty()) {
                continue;
            }

            for (Address target : liveEndpoints) {
                batchReplicationEventMap.put(target, new BatchWanReplicationEvent(snapshotEnabled));
            }

            if (!events.isEmpty()) {
                checkExecutorInitialized(liveEndpoints.size());
                for (WanReplicationEvent event : events) {
                    final EnterpriseReplicationEventObject eventObject
                            = (EnterpriseReplicationEventObject) event.getEventObject();
                    final int partitionId = getPartitionId(eventObject.getKey());
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

    /**
     * Returns a list of currently live endpoints. It will sleep until the list contains at least one endpoint
     * or {@link #running} is {@code false} (this publisher is shutting down) at which point it can return an
     * empty list.
     */
    private List<Address> awaitAndGetTargetEndpoints() {
        while (running) {
            final List<Address> endpoints = connectionManager.getTargetEndpoints();
            if (!endpoints.isEmpty()) {
                return endpoints;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
        return Collections.emptyList();
    }

    /** Drains the staging queue until enough items have been drained or enough time has passed */
    private List<WanReplicationEvent> drainStagingQueue() {
        List<WanReplicationEvent> wanReplicationEventList = new ArrayList<WanReplicationEvent>();
        while (!(wanReplicationEventList.size() >= batchSize || sendingPeriodPassed(wanReplicationEventList.size()))) {
            if (!running) {
                break;
            }
            WanReplicationEvent event = null;
            try {
                event = stagingQueue.poll(batchMaxDelayMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
            if (event != null) {
                wanReplicationEventList.add(event);
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
                logger.info("WanBatchReplication striped runnable job queue is full. Retrying.");
            }
        } while (!taskSubmitted && running);
    }

    /** Checks if {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} has passed since the last replication was sent */
    private boolean sendingPeriodPassed(int eventQueueSize) {
        return System.currentTimeMillis() - lastBatchSendTime > batchMaxDelayMillis && eventQueueSize > 0;
    }

    /**
     * Initializes the striped executor if necessary. If {@link WanReplicationProperties#EXECUTOR_THREAD_COUNT} is defined, it
     * will use that number of threads, otherwise it will use the {@code threadCount}.
     */
    private void checkExecutorInitialized(int threadCount) {
        if (executor == null) {
            synchronized (mutex) {
                if (executor == null) {
                    executor = new StripedExecutor(node.getLogger(WanBatchReplication.class),
                            createThreadName(node.hazelcastInstance.getName(), "wan-batch-replication"),
                            executorThreadCount > 0 ? executorThreadCount : threadCount,
                            STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
            }
        }
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
            boolean transmitSucceed = false;
            do {
                WanConnectionWrapper connectionWrapper = null;
                try {
                    connectionWrapper = connectionManager.getConnection(target);
                    if (connectionWrapper != null) {
                        boolean isTargetInvocationSuccessful = invokeOnWanTarget(
                                connectionWrapper.getConnection().getEndPoint(), batchReplicationEvent);
                        if (isTargetInvocationSuccessful) {
                            for (WanReplicationEvent event : batchReplicationEvent.getEvents()) {
                                removeReplicationEvent(event);
                            }
                            decrementWANQueueSize(batchReplicationEvent.getAddedEventCount());
                        } else {
                            failureCount.incrementAndGet();
                        }
                        transmitSucceed = isTargetInvocationSuccessful;
                    }
                } catch (Throwable t) {
                    logger.warning(t);
                    if (connectionWrapper != null) {
                        final Address address = connectionWrapper.getTargetAddress();
                        connectionManager.removeTargetEndpoint(address,
                                "Error occurred when sending WAN events to " + address, t);
                    }
                }
            } while (!transmitSucceed && running);
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

    public long getFailureCount() {
        return failureCount.get();
    }
}
