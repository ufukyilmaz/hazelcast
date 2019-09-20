package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.enterprise.wan.impl.operation.WanOperation;
import com.hazelcast.enterprise.wan.impl.replication.BatchWanReplicationEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ReplicationSupportingService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.util.executor.StripedExecutor;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.config.ExecutorConfig.DEFAULT_POOL_SIZE;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;

/**
 * The class responsible for processing WAN events coming from a source
 * cluster.
 */
class WanEventProcessor implements LiveOperationsTracker {
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 1000;
    private static final int DEFAULT_KEY_FOR_STRIPED_EXECUTORS = -1;
    /** Mutex for creating the executor for processing incoming WAN events */
    private final Object executorMutex = new Object();
    private final ILogger logger;
    private final Node node;
    /**
     * Operations which are processed on threads other than the operation
     * thread. We must report these operations to the operation system for it
     * to send operation heartbeats to the operation sender.
     */
    private final Set<Operation> liveOperations = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile StripedExecutor executor;

    WanEventProcessor(Node node) {
        this.logger = node.getLogger(WanEventProcessor.class.getName());
        this.node = node;
    }

    /**
     * Processes the {@code replicationEvent} by offloading it to a separate
     * thread. The WAN operation will be notified of the processing result.
     *
     * @param replicationEvent the WAN replication events to process
     * @param op               the operation which will be notified of the
     *                         processing result
     */
    public void handleRepEvent(BatchWanReplicationEvent replicationEvent, WanOperation op) {
        Collection<InternalWanReplicationEvent> eventList = replicationEvent.getEvents();
        int partitionId = eventList.isEmpty()
                ? DEFAULT_KEY_FOR_STRIPED_EXECUTORS
                : getPartitionId(eventList.iterator().next().getKey());
        BatchWanEventRunnable processingRunnable = new BatchWanEventRunnable(
                replicationEvent, op, partitionId, node.getNodeEngine(), liveOperations, logger);
        executeAndNotify(processingRunnable, op);
    }

    /**
     * Processes the {@code event} by offloading it to a separate
     * thread. The WAN operation will be notified of the processing result.
     *
     * @param event the WAN replication event to process
     * @param op    the operation which will be notified of the
     *              processing result
     */
    public void handleRepEvent(InternalWanReplicationEvent event, WanOperation op) {
        final int partitionId = getPartitionId(event.getKey());
        final WanEventRunnable processingRunnable
                = new WanEventRunnable(event, op, partitionId, node.getNodeEngine(), liveOperations, logger);
        executeAndNotify(processingRunnable, op);
    }

    public void handleEvent(WanReplicationEvent event, WanAcknowledgeType acknowledgeType) {
        String serviceName = event.getServiceName();
        ReplicationSupportingService service = node.getNodeEngine().getService(serviceName);
        service.onReplicationEvent(event, acknowledgeType);
    }

    /**
     * Executes the {@code wanProcessingRunnable} by offloading it to a separate
     * thread. The WAN operation will be notified of the processing result.
     *
     * @param wanProcessingRunnable the runnable to execute
     * @param op                    the operation which will be notified of the
     *                              processing result
     */
    private void executeAndNotify(Runnable wanProcessingRunnable, WanOperation op) {
        final StripedExecutor ex = getExecutor();
        try {
            liveOperations.add(op);
            ex.execute(wanProcessingRunnable);
        } catch (RejectedExecutionException ree) {
            logger.warning("Can not handle incoming wan replication event.", ree);
            try {
                op.sendResponse(false);
            } finally {
                liveOperations.remove(op);
            }
        }
    }

    private StripedExecutor getExecutor() {
        StripedExecutor ex = executor;
        if (ex == null) {
            synchronized (executorMutex) {
                if (executor == null) {
                    String prefix = createThreadName(node.hazelcastInstance.getName(), "wan");
                    executor = new StripedExecutor(logger, prefix,
                            DEFAULT_POOL_SIZE, STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
                ex = executor;
            }
        }
        return ex;
    }

    /** Returns the partition ID for the partition owning the {@code key} */
    private int getPartitionId(Data key) {
        return node.getNodeEngine().getPartitionService().getPartitionId(key);
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        // populate for all WanOperation
        for (Operation op : this.liveOperations) {
            liveOperations.add(op.getCallerAddress(), op.getCallId());
        }
    }

    public void shutdown() {
        StripedExecutor ex = executor;
        if (ex != null) {
            ex.shutdown();
        }
    }
}
