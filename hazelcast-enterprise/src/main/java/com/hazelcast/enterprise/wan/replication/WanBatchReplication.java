package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_SIZE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.SNAPSHOT_ENABLED;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;
import static com.hazelcast.util.ThreadUtil.createThreadName;

/**
 * WAN replication publisher that sends events in batches.
 * Basically, it publishes events either when enough events are enqueued or enqueued events have waited for enough time.
 * <p>
 * The event count is configurable by {@link WanReplicationProperties#BATCH_SIZE} and is {@value DEFAULT_BATCH_SIZE} by default.
 * The elapsed time is configurable by {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} and is
 * {@value DEFAULT_BATCH_MAX_DELAY_MILLIS} by default.
 * The events are sent to the {@link WanReplicationProperties#ENDPOINTS} depending on the event key partition.
 */
public class WanBatchReplication extends AbstractWanReplication implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private final List<String> targets = new ArrayList<String>();
    private final Object mutex = new Object();

    private volatile StripedExecutor executor;
    private volatile long lastBatchSendTime = System.currentTimeMillis();

    private boolean snapshotEnabled;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
        super.init(node, wanReplicationConfig, wanPublisherConfig);
        Map<String, Comparable> props = wanPublisherConfig.getProperties();
        targets.addAll(endpointList);
        snapshotEnabled = getProperty(SNAPSHOT_ENABLED, props, false);

        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    @Override
    protected void afterShutdown() {
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
            Map<String, BatchWanReplicationEvent> batchReplicationEventMap;
            List<WanReplicationEvent> wanReplicationEventList = drainStagingQueue();

            batchReplicationEventMap = new ConcurrentHashMap<String, BatchWanReplicationEvent>();
            for (String target : targets) {
                batchReplicationEventMap.put(target, new BatchWanReplicationEvent(snapshotEnabled));
            }

            if (!wanReplicationEventList.isEmpty()) {
                for (WanReplicationEvent wanReplicationEvent : wanReplicationEventList) {
                    EnterpriseReplicationEventObject event
                            = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
                    String target = getTarget(event.getKey());
                    batchReplicationEventMap.get(target).addEvent(wanReplicationEvent);
                }
                for (Map.Entry<String, BatchWanReplicationEvent> entry : batchReplicationEventMap.entrySet()) {
                    BatchWanReplicationEvent event = entry.getValue();
                    if (event.getEventList().size() > 0) {
                        handleBatchReplicationEventObject(entry.getKey(), entry.getValue());
                    }
                }
                lastBatchSendTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Drains the staging queue until enough items have been drained or enough time has passed
     */
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
                EmptyStatement.ignore(ignored);
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
    private void handleBatchReplicationEventObject(final String target, final BatchWanReplicationEvent batchReplicationEvent) {
        StripedExecutor ex = getExecutor();
        boolean taskSubmitted = false;
        BatchStripedRunnable batchStripedRunnable = new BatchStripedRunnable(target, batchReplicationEvent);
        do {
            try {
                ex.execute(batchStripedRunnable);
                taskSubmitted = true;
            } catch (RejectedExecutionException ree) {
                logger.info("WanBatchReplication striped runnable job queue is full. Retrying.");
            }
        } while (!taskSubmitted && running);
    }

    private String getTarget(Data key) {
        int index = getPartitionId(key) % targets.size();
        return targets.get(index);
    }

    /**
     * Checks if {@link WanReplicationProperties#BATCH_MAX_DELAY_MILLIS} has passed since the last replication was sent
     */
    private boolean sendingPeriodPassed(int eventQueueSize) {
        return System.currentTimeMillis() - lastBatchSendTime > batchMaxDelayMillis && eventQueueSize > 0;
    }

    private StripedExecutor getExecutor() {
        StripedExecutor ex = executor;
        if (ex == null) {
            synchronized (mutex) {
                if (executor == null) {
                    executor = new StripedExecutor(node.getLogger(WanBatchReplication.class),
                            createThreadName(node.hazelcastInstance.getName(), "wan-batch-replication"),
                            targets.size(),
                            STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
                ex = executor;
            }
        }
        return ex;
    }

    /**
     * {@link StripedRunnable} implementation to send Batch of WAN replication events to target cluster. It will retry
     * sending the event until it has succeeded or until stopped. The WAN event backups are removed from the replicas
     * after the batch has been sent and operation response received from the target cluster.
     */
    private class BatchStripedRunnable implements StripedRunnable, TimeoutRunnable {

        private String target;
        private BatchWanReplicationEvent batchReplicationEvent;

        public BatchStripedRunnable(String target, BatchWanReplicationEvent batchReplicationEvent) {
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
                    Connection conn = connectionWrapper.getConnection();
                    if (conn != null && conn.isAlive()) {
                        boolean isTargetInvocationSuccessful = invokeOnWanTarget(conn.getEndPoint(), batchReplicationEvent);
                        if (isTargetInvocationSuccessful) {
                            for (WanReplicationEvent event : batchReplicationEvent.getEventList()) {
                                removeReplicationEvent(event);
                            }
                        }
                        transmitSucceed = isTargetInvocationSuccessful;
                    }
                } catch (Throwable t) {
                    logger.warning(t);
                    if (connectionWrapper != null) {
                        connectionManager.reportFailedConnection(connectionWrapper.getTargetAddress());
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
}
