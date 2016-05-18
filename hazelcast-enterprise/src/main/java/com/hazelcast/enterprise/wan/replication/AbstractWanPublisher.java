package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.sync.PartitionSyncReplicationEventObject;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract WAN event publisher implementation.
 */
public abstract class AbstractWanPublisher
        implements WanReplicationPublisher, WanReplicationEndpoint {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);
    private static final int DEFAULT_STAGING_QUEUE_SIZE = 100;

    protected volatile boolean running = true;
    protected volatile boolean paused;
    protected volatile WanPublisherConfig publisherConfig;

    protected String targetGroupName;
    protected String localGroupName;
    protected String wanReplicationName;
    protected Node node;
    protected int queueCapacity;
    protected WANQueueFullBehavior queueFullBehavior;

    protected long lastQueueFullLogTimeMs;

    protected int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;

    protected PublisherQueueContainer eventQueueContainer;
    protected BlockingQueue<WanReplicationEvent> stagingQueue;

    protected ILogger logger;

    private final LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();

    private final AtomicInteger currentElementCount = new AtomicInteger(0);

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        publishReplicationEvent(serviceName, eventObject);
    }

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        this.node = node;
        this.publisherConfig = publisherConfig;
        this.targetGroupName = publisherConfig.getGroupName();
        this.wanReplicationName = wanReplicationConfig.getName();
        this.logger = node.getLogger(getClass());

        this.queueCapacity = publisherConfig.getQueueCapacity();
        this.localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();

        this.eventQueueContainer = new PublisherQueueContainer(node);
        this.stagingQueue = new ArrayBlockingQueue<WanReplicationEvent>(getStagingQueueSize());
        this.queueFullBehavior = publisherConfig.getQueueFullBehavior();

        node.nodeEngine.getExecutionService().execute("hz:wan:poller", new QueuePoller());
    }

    public int getStagingQueueSize() {
        return DEFAULT_STAGING_QUEUE_SIZE;
    }

    protected int getPartitionId(Object key) {
        return  node.nodeEngine.getPartitionService().getPartitionId(key);
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        boolean dropEvent = isEventDroppingNeeded();
        if (!dropEvent) {
            EnterpriseReplicationEventObject replicationEventObject = (EnterpriseReplicationEventObject) eventObject;
            if (!replicationEventObject.getGroupNames().contains(targetGroupName)) {
                replicationEventObject.getGroupNames().add(localGroupName);
                WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);
                int partitionId = getPartitionId(((EnterpriseReplicationEventObject) eventObject).getKey());
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
            String cacheName = cacheReplicationObject.getNameWithPrefix();
            if (dropEvent) {
                eventQueueContainer.pollCacheWanEvent(cacheName, partitionId);
            }
            eventPublished = eventQueueContainer.publishCacheWanEvent(cacheName, partitionId, replicationEvent);
        } else if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            String mapName = mapReplicationObject.getMapName();
            if (dropEvent) {
                eventQueueContainer.pollMapWanEvent(mapName, partitionId);
            }
            eventPublished = eventQueueContainer.publishMapWanEvent(mapName, partitionId, replicationEvent);
        } else {
            logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
        }
        return eventPublished;
    }

    private boolean isEventDroppingNeeded() {
        boolean dropEvent = false;
        if (currentElementCount.get() >= queueCapacity
                && queueFullBehavior == WANQueueFullBehavior.DISCARD_AFTER_MUTATION) {
            long curTime = System.currentTimeMillis();
            if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                lastQueueFullLogTimeMs = curTime;
                logger.severe("Wan replication event queue is full. Dropping events. Queue size : "
                        + currentElementCount.get());
            } else {
                logger.finest("Wan replication event queue is full. An event will be dropped.");
            }
            dropEvent = true;
        }
        return dropEvent;
    }

    public void removeReplicationEvent(WanReplicationEvent wanReplicationEvent) {

        if (wanReplicationEvent.getEventObject() instanceof EnterpriseMapReplicationSync) {
            return;
        }
        removeLocal();
        updateStats(wanReplicationEvent);
        Data wanReplicationEventData
                = node.nodeEngine.getSerializationService().toData(wanReplicationEvent);
        Operation ewrRemoveOperation
                = new EWRRemoveBackupOperation(wanReplicationName, targetGroupName, wanReplicationEventData);
        OperationService operationService = node.nodeEngine.getOperationService();
        EnterpriseReplicationEventObject evObj
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        int backupCount = evObj.getBackupCount();
        int clusterSize = node.getClusterService().getSize();
        int partitionId = getPartitionId(evObj.getKey());
        List<InternalCompletableFuture> futures = new ArrayList<InternalCompletableFuture>(backupCount);
        try {
            for (int i = 0; i < backupCount && i < clusterSize - 1; i++) {
                InternalCompletableFuture<Object> future = operationService
                        .createInvocationBuilder(EnterpriseWanReplicationService.SERVICE_NAME,
                                ewrRemoveOperation,
                                partitionId)
                        .setResultDeserialized(false)
                        .setReplicaIndex(i + 1)
                        .invoke();
                futures.add(future);
            }
            for (InternalCompletableFuture future : futures) {
                future.get();
            }
        } catch (Exception ex) {
            logger.warning("Exception while removing wan backup", ex);
        }
    }

    private void updateStats(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        long latency = Clock.currentTimeMillis() - eventObject.getCreationTime();
        localWanPublisherStats.incrementPublishedEventCount(latency);
    }

    private void removeLocal() {
        currentElementCount.decrementAndGet();
    }

    @Override
    public void removeBackup(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        int partitionId = getPartitionId(eventObject.getKey());
        WanReplicationEvent wanEvent = null;
        if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
            String cacheName = cacheReplicationObject.getNameWithPrefix();
            wanEvent = eventQueueContainer.pollCacheWanEvent(cacheName, partitionId);
        } else if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            String mapName = mapReplicationObject.getMapName();
            wanEvent = eventQueueContainer.pollMapWanEvent(mapName, partitionId);
        } else {
            logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
        }

        if (wanEvent != null) {
            currentElementCount.decrementAndGet();
        }
    }

    @Override
    public void putBackup(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        publishReplicationEvent(wanReplicationEvent.getServiceName(), eventObject);
    }

    @Override
    public PublisherQueueContainer getPublisherQueueContainer() {
        return eventQueueContainer;
    }

    @Override
    public void addMapQueue(String name, int partitionId, WanReplicationEventQueue eventQueue) {
        WanReplicationEvent event = eventQueue.poll();
        while (event != null) {
            publishReplicationEvent(event.getServiceName(), event.getEventObject());
            event = eventQueue.poll();
        }
    }

    @Override
    public void addCacheQueue(String name, int partitionId, WanReplicationEventQueue eventQueue) {
        WanReplicationEvent event = eventQueue.poll();
        while (event != null) {
            publishReplicationEvent(event.getServiceName(), event.getEventObject());
            event = eventQueue.poll();
        }
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
        localWanPublisherStats.setConnected(isConnected());
        localWanPublisherStats.setOutboundQueueSize(currentElementCount.get());
        return localWanPublisherStats;
    }

    public abstract boolean isConnected();

    @Override
    public void checkWanReplicationQueues() {
        if (WANQueueFullBehavior.THROW_EXCEPTION == queueFullBehavior
                && currentElementCount.get() >= queueCapacity) {
            throw new WANReplicationQueueFullException(
                    String.format("WAN replication for target cluster %s is full. Queue capacity is %d",
                            targetGroupName, queueCapacity));
        }
    }

    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject replicationEventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        EWRPutOperation ewrPutOperation =
                new EWRPutOperation(wanReplicationName, targetGroupName,
                        node.nodeEngine.toData(wanReplicationEvent),
                        replicationEventObject.getBackupCount());
        invokeOnPartition(wanReplicationEvent.getServiceName(), replicationEventObject.getKey(), ewrPutOperation);
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

    public String getTargetGroupName() {
        return targetGroupName;
    }

    @Override
    public void publishMapSyncEvent(PartitionSyncReplicationEventObject eventObject) {
        WanReplicationEvent wanReplicationEvent = new WanReplicationEvent(MapService.SERVICE_NAME, eventObject);
        eventQueueContainer.publishMapWanEvent(eventObject.getMapName(), eventObject.getPartitionId(), wanReplicationEvent);
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
                    for (IPartition partition : node.getPartitionService().getPartitions()) {
                        if (!partition.isLocal()) {
                            continue;
                        }

                        WanReplicationEvent event = eventQueueContainer.pollRandomWanEvent(partition.getPartitionId());
                        if (event == null) {
                            continue;
                        }

                        offered = false;
                        while (!offered && running) {
                            try {
                                handleEvent(event);
                                offered = true;
                                emptyIterationCount = 0;
                            } catch (InterruptedException ignored) {
                                EmptyStatement.ignore(ignored);
                            }
                        }
                    }
                }

                if (!offered && running) {
                    int sleepMs = ++emptyIterationCount * SLEEP_INTERVAL_MS;
                    try {
                        Thread.sleep(Math.min(sleepMs, MAX_SLEEP_MS));
                    } catch (InterruptedException ignored) {
                        EmptyStatement.ignore(ignored);
                    }
                }
            }
        }

        private void handleEvent(WanReplicationEvent event) throws InterruptedException {
            if (event.getEventObject() instanceof PartitionSyncReplicationEventObject) {
                PartitionSyncReplicationEventObject evObj = (PartitionSyncReplicationEventObject) event.getEventObject();
                for (EntryView entryView : evObj.getEntryViews()) {
                    EnterpriseMapReplicationSync sync = new EnterpriseMapReplicationSync(
                            evObj.getMapName(), entryView);
                    WanReplicationEvent wanEvent = new WanReplicationEvent(event.getServiceName(), sync);
                    stagingQueue.put(wanEvent);
                }
            } else {
                stagingQueue.put(event);
            }
        }
    }
}
