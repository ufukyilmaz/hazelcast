package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
        implements Runnable, WanReplicationEndpoint {

    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 50;

    private ILogger logger;
    private Queue<WanReplicationEvent> eventQueue;
    private volatile boolean running = true;
    private List<String> targets = new ArrayList<String>();

    private volatile long lastBatchSendTime = System.currentTimeMillis();

    private final Object mutex = new Object();
    private final Object queueMonitor = new Object();
    private volatile StripedExecutor executor;

    @Override
    public void init(Node node, String groupName, String password, boolean snapshotEnabled, String... targets) {
        super.init(node, groupName, password, snapshotEnabled, targets);
        eventQueue = new LinkedList<WanReplicationEvent>();
        logger = node.getLogger(WanBatchReplication.class.getName());
        this.targets.addAll(Arrays.asList(targets));
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);
        EnterpriseReplicationEventObject replicationEventObject = (EnterpriseReplicationEventObject) eventObject;
        if (!replicationEventObject.getGroupNames().contains(targetGroupName)) {
            replicationEventObject.getGroupNames().add(localGroupName);
            synchronized (queueMonitor) {
                if (eventQueue.size() >= queueSize) {
                    long curTime = System.currentTimeMillis();
                    if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                        lastQueueFullLogTimeMs = curTime;
                        logger.severe("Wan replication event queue is full. Dropping events.");
                    } else {
                        logger.finest("Wan replication event queue is full. An event is dropped.");
                    }
                    //the replication event could not be published because the eventQueue is full. So we are going
                    //to drain one item and then offer it again.
                    eventQueue.poll();
                }
                eventQueue.offer(replicationEvent);
                queueMonitor.notify();
            }
        }
    }

    public void shutdown() {
        running = false;
        StripedExecutor ex = executor;
        if (ex != null) {
            ex.shutdown();
        }
    }

    public void run() {
        while (running) {

            List<WanReplicationEvent> wanReplicationEventList = null;
            Map<String, BatchWanReplicationEvent> batchReplicationEventMap = null;

            synchronized (queueMonitor) {
                int eventQueueSize = eventQueue.size();
                if (eventQueueSize > batchSize
                        || sendingPeriodPassed(eventQueueSize)) {
                    batchReplicationEventMap = new ConcurrentHashMap<String, BatchWanReplicationEvent>();
                    for (String target : targets) {
                        batchReplicationEventMap.put(target, new BatchWanReplicationEvent(snapshotEnabled));
                    }
                    wanReplicationEventList = new ArrayList<WanReplicationEvent>();
                    drainTo(eventQueue, wanReplicationEventList, batchSize);
                } else {
                    try {
                        queueMonitor.wait(batchFrequency);
                    } catch (InterruptedException e) {
                        logger.finest("Queue monitor interrupted");
                    }
                }
            }

            if (wanReplicationEventList != null) {
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

    private void drainTo(Queue<WanReplicationEvent> eventQueue,
                         List<WanReplicationEvent> wanReplicationEventList, int batchSize) {
        int eventQueueSize = eventQueue.size();
        int count = eventQueueSize < batchSize ? eventQueueSize : batchSize;
        for (int i = 0; i < count; i++) {
            wanReplicationEventList.add(eventQueue.poll());
        }
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
        } while (!taskSubmitted);
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
                WanConnectionWrapper connectionWrapper = null;
                try {
                    connectionWrapper = connectionManager.getConnection(target);
                    Connection conn = connectionWrapper.getConnection();
                    if (conn != null && conn.isAlive()) {
                        try {
                            invokeOnWanTarget(conn.getEndPoint(), batchReplicationEvent).get();
                            transmitSucceed = true;
                        } catch (Exception ignored) {
                            logger.warning(ignored);
                        }
                    }
                } catch (Throwable e) {
                    if (logger != null) {
                        logger.warning(e);
                    }
                    if (connectionWrapper != null) {
                        connectionManager.reportFailedConnection(connectionWrapper.getTargetAddress());
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
