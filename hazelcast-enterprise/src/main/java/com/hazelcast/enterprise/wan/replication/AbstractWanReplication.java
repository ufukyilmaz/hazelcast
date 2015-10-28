package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

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
    volatile boolean paused;

    String targetGroupName;
    String localGroupName;
    String wanReplicationName;
    boolean snapshotEnabled;
    Node node;
    int queueSize;
    WanConnectionManager connectionManager;
    WanAcknowledgeType acknowledgeType;

    int batchSize;
    long batchFrequency;
    long operationTimeout;
    long lastQueueFullLogTimeMs;

    int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;

    PublisherQueueContainer eventQueueContainer;
    BlockingQueue<WanReplicationEvent> stagingQueue;

    private Object queueMonitor = new Object();
    private LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();

    private ILogger logger;

    private final AtomicInteger currentElementCount = new AtomicInteger(0);

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        publishReplicationEvent(serviceName, eventObject);
    }

    public void init(Node node, String wanReplicationName, WanTargetClusterConfig targetClusterConfig, boolean snapshotEnabled) {
        this.node = node;
        this.targetGroupName = targetClusterConfig.getGroupName();
        this.snapshotEnabled = snapshotEnabled;
        this.wanReplicationName = wanReplicationName;
        this.logger = node.getLogger(AbstractWanReplication.class.getName());

        this.queueSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY);
        localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();

        batchSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_BATCH_SIZE);
        batchFrequency = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS);
        operationTimeout = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS);

        connectionManager = new WanConnectionManager(node);
        connectionManager.init(targetGroupName, targetClusterConfig.getGroupPassword(), targetClusterConfig.getEndpoints());

        eventQueueContainer = new PublisherQueueContainer(node);
        stagingQueue = new ArrayBlockingQueue<WanReplicationEvent>(batchSize);
        acknowledgeType = targetClusterConfig.getAcknowledgeType();

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

    protected void invokeOnWanTarget(Address target, DataSerializable event, WanAcknowledgeType acknowledgeType) {
        Operation wanOperation
                = new WanOperation(node.nodeEngine.getSerializationService().toData(event));
        OperationService operationService = node.nodeEngine.getOperationService();
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        InvocationBuilder invocationBuilder
                = operationService.createInvocationBuilder(serviceName, wanOperation, target);
        Future future = invocationBuilder.setTryCount(1)
                .setCallTimeout(operationTimeout)
                .invoke();
        if (acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            try {
                future.get();
            } catch (Exception ex) {
                ExceptionUtil.rethrow(ex);
            }
        }
    }

    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        EWRPutOperation ewrPutOperation = new EWRPutOperation(wanReplicationName,
                targetGroupName, node.nodeEngine.toData(wanReplicationEvent), 1);
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
                boolean dropEvent = isEventDroppingNeeded();
                boolean eventPublished = publishEventInternal(eventObject, replicationEvent, partitionId, dropEvent);
                if (eventPublished) {
                    currentElementCount.incrementAndGet();
                }
            }
        }
    }

    private boolean publishEventInternal(ReplicationEventObject eventObject, WanReplicationEvent replicationEvent,
                                         int partitionId, boolean dropEvent) {
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
        return eventPublished;
    }

    private boolean isEventDroppingNeeded() {
        boolean dropEvent = false;
        if (currentElementCount.get() >= queueSize) {
            long curTime = System.currentTimeMillis();
            if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                lastQueueFullLogTimeMs = curTime;
                logger.severe("Wan replication event queue is full. Dropping events.");
            } else {
                logger.finest("Wan replication event queue is full. An event will be dropped.");
            }
            dropEvent = true;
        }
        return dropEvent;
    }

    public void removeReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        removeLocal();
        updateStats(wanReplicationEvent);
        Operation ewrRemoveOperation = new EWRRemoveBackupOperation(wanReplicationName,
                targetGroupName,
                node.nodeEngine.getSerializationService().toData(wanReplicationEvent));
        OperationService operationService = node.nodeEngine.getOperationService();
        EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        int backupCount = evObj.getBackupCount();
        int clusterSize = node.getClusterService().getSize();
        for (int i = 0; i < backupCount && i < clusterSize - 1; i++) {
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

    private void updateStats(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        long latency = Clock.currentTimeMillis() - eventObject.getCreationTime();
        localWanPublisherStats.incrementPublishedEventCount(latency);
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

    public String getTargetGroupName() {
        return targetGroupName;
    }

    @Override
    public PublisherQueueContainer getPublisherQueueContainer() {
        return eventQueueContainer;
    }

    @Override
    public void addMapQueue(String name, int partitionId, WanReplicationEventQueue eventQueue) {
        eventQueueContainer.getPublisherEventQueueMap().get(partitionId)
                .getMapWanEventQueueMap().put(name, eventQueue);
    }

    @Override
    public void addCacheQueue(String name, int partitionId, WanReplicationEventQueue eventQueue) {
        eventQueueContainer.getPublisherEventQueueMap().get(partitionId)
                .getCacheWanEventQueueMap().put(name, eventQueue);
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
    }

    @Override
    public LocalWanPublisherStats getStats() {
        localWanPublisherStats.setPaused(paused);
        localWanPublisherStats.setConnected(connectionManager.getFailedAddressSet().isEmpty());
        localWanPublisherStats.setOutboundQueueSize(currentElementCount.get());
        return localWanPublisherStats;
    }

    private class QueuePoller implements Runnable {

        private static final int MAX_SLEEP_MS = 2000;
        private static final int SLEEP_INTERVAL_MS = 200;
        private int emptyIterationCount;

        @Override
        public void run() {
            while (running) {

                boolean offered = false;

                if (!paused) {
                    for (InternalPartition partition : node.getPartitionService().getPartitions()) {
                        if (!partition.isLocal()) {
                            continue;
                        }

                        WanReplicationEvent event = eventQueueContainer.pollRandomWanEvent(partition.getPartitionId());
                        if (event == null) {
                            continue;
                        }
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
