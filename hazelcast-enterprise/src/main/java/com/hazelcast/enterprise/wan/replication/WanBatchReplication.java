package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Wan replication publisher that sends events in batches.
 * <p>Basically, it either publishes events either when a pre-defined
 * (see {@link com.hazelcast.instance.GroupProperties#ENTERPRISE_WAN_REP_BATCH_SIZE}) number of events are enqueued
 * or
 * enqueued events are waited enough (see
 * {@link com.hazelcast.instance.GroupProperties#PROP_ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS}
 * </p>
 */
public class WanBatchReplication extends AbstractWanReplication
        implements Runnable {

    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private ILogger logger;
    private List<String> targets = new ArrayList<String>();

    private volatile long lastBatchSendTime = System.currentTimeMillis();

    private final Object mutex = new Object();
    private volatile StripedExecutor executor;

    @Override
    public void init(Node node, String groupName, String password, boolean snapshotEnabled,
                     String wanReplicationName, String... targets) {
        super.init(node, groupName, password, snapshotEnabled, wanReplicationName, targets);
        logger = node.getLogger(WanBatchReplication.class.getName());
        this.targets.addAll(Arrays.asList(targets));
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
            WanReplicationEvent event = null;
            try {
                 event = stagingQueue.poll(batchFrequency, TimeUnit.MILLISECONDS);
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
        } while (!taskSubmitted && ex.isLive());
    }

    private String getTarget(Data key) {
        int index = getPartitionId(key) % targets.size();
        return targets.get(index);
    }

    private boolean sendingPeriodPassed(int eventQueueSize) {
        return System.currentTimeMillis() - lastBatchSendTime > batchFrequency
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
                WanConnectionWrapper connectionWrapper;
                try {
                    connectionWrapper = connectionManager.getConnection(target);
                    Connection conn = connectionWrapper.getConnection();
                    if (conn != null && conn.isAlive()) {
                        try {
                            invokeOnWanTarget(conn.getEndPoint(), batchReplicationEvent).get();
                            for (WanReplicationEvent event : batchReplicationEvent.getEventList()) {
                                removeReplicationEvent(event);
                            }
                            transmitSucceed = true;
                        } catch (Exception ignored) {
                            logger.warning(ignored);
                        }
                    }
                } catch (Throwable e) {
                    if (logger != null) {
                        logger.warning(e);
                    }
                }
            } while (!transmitSucceed);
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
