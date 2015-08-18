package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.WanReplicationEventQueueContainer;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract WAN event publisher implementation
 */
public abstract class AbstractWanReplication
        implements WanReplicationPublisher, WanReplicationEndpoint {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

    volatile boolean running = true;

    String targetGroupName;
    String localGroupName;
    String wanReplicationName;
    boolean snapshotEnabled;
    Node node;
    int queueSize;
    WanConnectionManager connectionManager;

    int batchSize;
    long batchFrequency;
    long operationTimeout;
    long lastQueueFullLogTimeMs;

    int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;

    WanReplicationEventQueueContainer eventQueueContainer;
    BlockingQueue<WanReplicationEvent> stagingQueue;

    private Object queueMonitor = new Object();

    private ILogger logger;

    private final AtomicInteger currentElementCount = new AtomicInteger(0);

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        publishReplicationEvent(serviceName, eventObject);
    }

    public void init(Node node, String groupName, String password, boolean snapshotEnabled,
                     String wanReplicationName, String... targets) {
        this.node = node;
        this.targetGroupName = groupName;
        this.snapshotEnabled = snapshotEnabled;
        this.wanReplicationName = wanReplicationName;
        this.logger = node.getLogger(AbstractWanReplication.class.getName());

        this.queueSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY);
        localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();

        batchSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_BATCH_SIZE);
        batchFrequency = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS);
        operationTimeout = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS);

        connectionManager = new WanConnectionManager(node);
        connectionManager.init(groupName, password, Arrays.asList(targets));

        eventQueueContainer = new WanReplicationEventQueueContainer(node);
        stagingQueue = new ArrayBlockingQueue<WanReplicationEvent>(batchSize);

        node.nodeEngine.getExecutionService().execute("hz:wan:poller", new QueuePoller());

    }

    int getPartitionId(Object key) {
        return  node.nodeEngine.getPartitionService().getPartitionId(key);
    }

    private void invokeOnPartition(String serviceName, Data key, Operation operation) {
        try {
            int partitionId = node.nodeEngine.getPartitionService().getPartitionId(key);
            node.nodeEngine.getOperationService()
                    .invokeOnPartition(serviceName, operation, partitionId).get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Future<Boolean> invokeOnWanTarget(Address target, DataSerializable event) {
        Operation wanOperation
                = new WanOperation(node.nodeEngine.getSerializationService().toData(event));
        OperationService operationService = node.nodeEngine.getOperationService();
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        InvocationBuilder invocationBuilder
                = operationService.createInvocationBuilder(serviceName, wanOperation, target);
        return invocationBuilder.setTryCount(1)
                .setCallTimeout(operationTimeout)
                .invoke();
    }

    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        EWRPutOperation ewrPutOperation = new EWRPutOperation(wanReplicationName,
                targetGroupName, node.nodeEngine.toData(wanReplicationEvent), wanReplicationEvent.getBackupCount());
        invokeOnPartition(wanReplicationEvent.getServiceName(),
                ((EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject()).getKey(), ewrPutOperation);
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        EnterpriseReplicationEventObject replicationEventObject = (EnterpriseReplicationEventObject) eventObject;
        if (!replicationEventObject.getGroupNames().contains(targetGroupName)) {
            replicationEventObject.getGroupNames().add(localGroupName);
            WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);
            int partitionId = getPartitionId(((EnterpriseReplicationEventObject) eventObject).getKey());
            synchronized (queueMonitor) {
                boolean dropEvent = false;
                if (currentElementCount.get() >= queueSize) {
                    dropEvent = true;
                    long curTime = System.currentTimeMillis();
                    if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                        lastQueueFullLogTimeMs = curTime;
                        logger.severe("Wan replication event queue is full. Dropping events.");
                    } else {
                        logger.finest("Wan replication event queue is full. An event will be dropped.");
                    }
                }
                boolean eventPublished = false;
                if (eventObject instanceof CacheReplicationObject) {
                    CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
                    if (dropEvent) {
                        WanReplicationEvent droppedEvent = eventQueueContainer.pollCacheWanEvent(
                                cacheReplicationObject.getNameWithPrefix(), partitionId);
                        if (droppedEvent != null) {
                            removeReplicationEvent(droppedEvent);
                        }
                    }
                    eventPublished = eventQueueContainer.publishCacheWanEvent(
                            cacheReplicationObject.getNameWithPrefix(), partitionId, replicationEvent);
                } else if (eventObject instanceof EnterpriseMapReplicationObject) {
                    EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
                    if (dropEvent) {
                        WanReplicationEvent droppedEvent =
                                eventQueueContainer.pollMapWanEvent(mapReplicationObject.getMapName(), partitionId);
                        if (droppedEvent != null) {
                            removeReplicationEvent(droppedEvent);
                        }
                    }
                    eventPublished = eventQueueContainer.publishMapWanEvent(
                            mapReplicationObject.getMapName(), partitionId, replicationEvent);
                } else {
                    logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
                }

                if (eventPublished) {
                    currentElementCount.incrementAndGet();
                }
            }
        }
    }

    public void removeReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        removeLocal();
        Operation ewrRemoveOperation = new EWRRemoveBackupOperation(wanReplicationName,
                targetGroupName,
                node.nodeEngine.getSerializationService().toData(wanReplicationEvent));
        OperationService operationService = node.nodeEngine.getOperationService();
        EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        for (int i = 0; i < evObj.getBackupCount() && i < node.getClusterService().getSize() - 1; i++) {
            try {
                operationService.createInvocationBuilder(EnterpriseWanReplicationService.SERVICE_NAME,
                        ewrRemoveOperation,
                        getPartitionId(evObj.getKey()))
                        .setResultDeserialized(false)
                        .setReplicaIndex(i + 1)
                        .invoke();
            } catch (Throwable t) {
                ExceptionUtil.rethrow(t);
            }
        }
    }

    private void removeLocal() {
        synchronized (queueMonitor) {
            currentElementCount.decrementAndGet();
        }
    }

    @Override
    public void removeBackup(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        int partitionId = getPartitionId(eventObject.getKey());
        if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
            eventQueueContainer.pollCacheWanEvent(cacheReplicationObject.getNameWithPrefix(), partitionId);
        } else if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            eventQueueContainer.pollMapWanEvent(mapReplicationObject.getMapName(), partitionId);
        } else {
            logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
        }
    }

    @Override
    public void putBackup(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        int partitionId = getPartitionId(eventObject.getKey());
        if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
            eventQueueContainer.publishCacheWanEvent(cacheReplicationObject.getNameWithPrefix(),
                    partitionId, wanReplicationEvent);
        } else if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            eventQueueContainer.publishMapWanEvent(mapReplicationObject.getMapName(), partitionId, wanReplicationEvent);
        } else {
            logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
        }
    }

    @Override
    public List<WanReplicationEventQueue> getEventQueueList(int partitionId) {
        List<WanReplicationEventQueue> eventQueues = new ArrayList<WanReplicationEventQueue>();
        Map<String, WanReplicationEventQueue> eventQueuesMap = eventQueueContainer.getEventQueueMapByPartitionId(partitionId);
        for (Map.Entry<String, WanReplicationEventQueue> eventQueueEntry : eventQueuesMap.entrySet()) {
            eventQueues.add(eventQueueEntry.getValue());
        }
        return eventQueues;
    }

    public WanReplicationEventQueueContainer getEventQueueContainer() {
        return eventQueueContainer;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public void shutdown() {
        running = false;
    }

    private class QueuePoller implements Runnable {

        private static final int MAX_SLEEP_MS = 2000;
        private static final int SLEEP_INTERVAL_MS = 200;
        private int emptyIterationCount;

        @Override
        public void run() {
            while (running) {

                boolean offered = false;

                for (InternalPartition partition : node.getPartitionService().getPartitions()) {
                    if (partition.isLocal()) {
                        WanReplicationEvent event = eventQueueContainer.pollRandomWanEvent(partition.getPartitionId());
                        if (event != null) {
                            offered = false;
                            while (!offered) {
                                try {
                                    stagingQueue.put(event);
                                    offered = true;
                                    emptyIterationCount = 0;
                                } catch (InterruptedException ignored) {
                                    EmptyStatement.ignore(ignored);
                                }
                            }
                        }
                    }
                }

                if (!offered) {
                    int sleepMs = ++emptyIterationCount * SLEEP_INTERVAL_MS;
                    try {
                        Thread.sleep(Math.min(sleepMs, MAX_SLEEP_MS));
                    } catch (InterruptedException ignored) {
                        EmptyStatement.ignore(ignored);
                    }
                }

            }
        }
    }
}
