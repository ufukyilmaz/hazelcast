package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.HazelcastThreadGroup;
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

/**
 * Wan replication publisher that sends events in batches.
 * <p>
 * Basically, it either publishes events either when a pre-defined
 * (see {@link WanTargetClusterConfig#batchSize}) number of events are enqueued
 * or enqueued events are waited enough (see {@link WanTargetClusterConfig#batchMaxDelayMillis}).
 * </p>
 */
public class WanBatchReplication
        extends AbstractWanReplication
        implements Runnable {

    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private final List<String> targets = new ArrayList<String>();
    private final Object mutex = new Object();

    private volatile StripedExecutor executor;
    private volatile long lastBatchSendTime = System.currentTimeMillis();

    @Override
    public void init(Node node, String wanReplicationName, WanTargetClusterConfig targetClusterConfig,
                     boolean snapshotEnabled) {
        super.init(node, wanReplicationName, targetClusterConfig, snapshotEnabled);
        targets.addAll(targetClusterConfig.getEndpoints());
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    public void shutdown() {
        super.shutdown();
        StripedExecutor ex = executor;
        if (ex != null) {
            ex.shutdown();
        }
    }

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

    private List<WanReplicationEvent> drainStagingQueue() {
        List<WanReplicationEvent> wanReplicationEventList = new ArrayList<WanReplicationEvent>();
        while (!(wanReplicationEventList.size() >= batchSize
                || sendingPeriodPassed(wanReplicationEventList.size()))) {
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

    private boolean sendingPeriodPassed(int eventQueueSize) {
        return System.currentTimeMillis() - lastBatchSendTime > batchMaxDelayMillis
                && eventQueueSize > 0;
    }

    private StripedExecutor getExecutor() {
        StripedExecutor ex = executor;
        if (ex == null) {
            synchronized (mutex) {
                if (executor == null) {
                    HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
                    executor = new StripedExecutor(node.getLogger(WanBatchReplication.class),
                            threadGroup.getThreadNamePrefix("wan-batch-replication"),
                            threadGroup.getInternalThreadGroup(),
                            targets.size(),
                            STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
                ex = executor;
            }
        }
        return ex;
    }

    /**
     * {@link StripedRunnable} implementation to send Batch of wan replication events to
     * target cluster
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
                        boolean isTargetInvocationSuccessful
                                = invokeOnWanTarget(conn.getEndPoint(), batchReplicationEvent);
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
