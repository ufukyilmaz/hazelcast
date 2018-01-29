package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PartitionWanEventContainer;
import com.hazelcast.enterprise.wan.PartitionWanEventQueueMap;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncResult;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Abstract WAN event publisher implementation.
 * This implementation prepares the WAN events from the WAN queues and
 * from WAN sync events for sending to the WAN endpoints.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractWanPublisher implements WanReplicationPublisher, WanReplicationEndpoint, LiveOperationsTracker {
    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);
    private static final int DEFAULT_STAGING_QUEUE_SIZE = 100;

    protected volatile boolean running = true;
    protected volatile boolean paused;
    protected volatile WanPublisherConfig publisherConfig;
    protected String targetGroupName;
    protected String localGroupName;
    protected String wanReplicationName;
    protected Node node;
    protected WanSyncManager syncManager;
    protected int queueCapacity;
    protected WANQueueFullBehavior queueFullBehavior;
    protected volatile long lastQueueFullLogTimeMs;
    protected int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;
    protected PublisherQueueContainer eventQueueContainer;
    /**
     * Queue for multiplexed map/cache partition WAN events that are about to
     * be transmitted to the target cluster
     */
    protected BlockingQueue<WanReplicationEvent> stagingQueue;
    protected BlockingQueue<WanSyncEvent> syncRequests = new ArrayBlockingQueue<WanSyncEvent>(DEFAULT_STAGING_QUEUE_SIZE);
    protected ILogger logger;
    private MapService mapService;
    private final LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();
    private final Set<Operation> liveOperations = newSetFromMap(new ConcurrentHashMap<Operation, Boolean>());
    private final AtomicInteger currentElementCount = new AtomicInteger(0);
    private final AtomicInteger currentBackupElementCount = new AtomicInteger(0);
    /** The count of ongoing {@link WanReplicationEvent} sync events per partition */
    private final Map<Integer, AtomicInteger> counterMap = new ConcurrentHashMap<Integer, AtomicInteger>();

    private volatile boolean resetCounters;
    private EnterpriseWanReplicationService wanService;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        this.node = node;
        this.mapService = node.nodeEngine.getService(MapService.SERVICE_NAME);
        this.publisherConfig = publisherConfig;
        this.targetGroupName = publisherConfig.getGroupName();
        this.wanReplicationName = wanReplicationConfig.getName();
        this.logger = node.getLogger(getClass());
        this.queueCapacity = publisherConfig.getQueueCapacity();
        this.localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();
        this.eventQueueContainer = new PublisherQueueContainer(node);
        this.stagingQueue = new ArrayBlockingQueue<WanReplicationEvent>(getStagingQueueSize());
        this.queueFullBehavior = publisherConfig.getQueueFullBehavior();
        this.wanService = (EnterpriseWanReplicationService) node.nodeEngine.getWanReplicationService();
        this.syncManager = wanService.getSyncManager();
        this.localWanPublisherStats.setSentMapEventCounter(
                unmodifiableMap(wanService.getSentEventCounter(MapService.SERVICE_NAME).getEventCounterMap()));
        this.localWanPublisherStats.setSentCacheEventCounter(
                unmodifiableMap(wanService.getSentEventCounter(ICacheService.SERVICE_NAME).getEventCounterMap()));

        node.nodeEngine.getExecutionService().execute("hz:wan:poller", new QueuePoller(syncManager));
    }

    public int getStagingQueueSize() {
        return DEFAULT_STAGING_QUEUE_SIZE;
    }

    protected int getPartitionId(Object key) {
        return node.nodeEngine.getPartitionService().getPartitionId(key);
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        publishReplicationEventInternal(serviceName, (EnterpriseReplicationEventObject) eventObject, false);
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        publishReplicationEventInternal(serviceName, (EnterpriseReplicationEventObject) eventObject, true);
    }

    private void publishReplicationEventInternal(String serviceName, EnterpriseReplicationEventObject eventObject,
                                                 boolean backupEvent) {
        if (isEventDroppingNeeded(backupEvent)) {
            if (!backupEvent) {
                wanService.getSentEventCounter(serviceName).incrementDropped(eventObject.getObjectName());
            }
            return;
        }
        if (eventObject.getGroupNames().contains(targetGroupName)) {
            return;
        }
        eventObject.getGroupNames().add(localGroupName);
        final WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);
        final int partitionId = getPartitionId(eventObject.getKey());
        final boolean eventPublished = publishEventInternal(eventObject, replicationEvent, partitionId, false);
        if (eventPublished) {
            incrementCounters(backupEvent);
        }
    }

    /**
     * Increments the counter for a single map or cache and event type.
     * The map/cache and event type is determined by the given {@code event}.
     *
     * @param event the WAN event
     */
    void incrementEventCount(WanReplicationEvent event) {
        final String serviceName = event.getServiceName();
        final ReplicationEventObject eventObject = event.getEventObject();
        eventObject.incrementEventCount(wanService.getSentEventCounter(serviceName));
    }

    private void incrementCounters(boolean backupEvent) {
        if (backupEvent) {
            currentBackupElementCount.incrementAndGet();
        } else {
            currentElementCount.incrementAndGet();
        }
    }

    /**
     * Publishes the {@code eventObject} and drops the oldest event if {@code dropEvent} is true.
     *
     * @param eventObject      the replication event
     * @param replicationEvent the event to be transmitted
     * @param partitionId      the partition ID on which this event is published
     * @param dropEvent        whether to drop the oldest event in the event queue
     * @return if the event has been published
     */
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

    /** Determine if queue capacity has been reached and if the event should be dropped */
    private boolean isEventDroppingNeeded(boolean isBackup) {
        if (isBackup) {
            if (currentBackupElementCount.get() >= queueCapacity) {
                return true;
            }
        } else if (currentElementCount.get() >= queueCapacity && queueFullBehavior != WANQueueFullBehavior.THROW_EXCEPTION) {
            long curTime = System.currentTimeMillis();
            if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                lastQueueFullLogTimeMs = curTime;
                logger.severe("Wan replication event queue is full. Dropping events. Queue size : " + currentElementCount.get());
            } else {
                logger.finest("Wan replication event queue is full. An event will be dropped.");
            }
            return true;
        }
        return false;
    }

    /**
     * Updates the WAN statistics (remaining map sync events, synced partitions, queued event count, latencies, sent events)
     * and removes the backup events on the replicas.
     *
     * @param wanReplicationEvent the completed replication event
     * @throws HazelcastSerializationException when the event fails to serialization
     */
    public void removeReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        if (wanReplicationEvent.getEventObject() instanceof EnterpriseMapReplicationSync) {
            EnterpriseMapReplicationSync sync = (EnterpriseMapReplicationSync) wanReplicationEvent.getEventObject();
            int remainingEventCount = counterMap.get(sync.getPartitionId()).decrementAndGet();
            if (remainingEventCount == 0) {
                syncManager.incrementSyncedPartitionCount();
            }
            return;
        }
        updateStats(wanReplicationEvent);
        final Data wanReplicationEventData = node.getSerializationService().toData(wanReplicationEvent);
        final OperationService operationService = node.nodeEngine.getOperationService();
        final EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        final int maxAllowedBackupCount = node.getPartitionService().getMaxAllowedBackupCount();
        final int backupCount = evObj.getBackupCount();
        final int partitionId = getPartitionId(evObj.getKey());
        final List<InternalCompletableFuture> futures = new ArrayList<InternalCompletableFuture>(backupCount);

        try {
            for (int i = 0; i < backupCount && i < maxAllowedBackupCount; i++) {
                Operation ewrRemoveOperation
                        = new EWRRemoveBackupOperation(wanReplicationName, targetGroupName, wanReplicationEventData);
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

    /** Update the published count and latency between event creation and publication */
    private void updateStats(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        long latency = Clock.currentTimeMillis() - eventObject.getCreationTime();
        localWanPublisherStats.incrementPublishedEventCount(latency < 0 ? 0 : latency);
    }

    protected void decrementWANQueueSize(int delta) {
        if (currentElementCount.get() > 0) {
            currentElementCount.addAndGet(-delta);
        }
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
            currentBackupElementCount.decrementAndGet();
        }
    }

    @Override
    public void putBackup(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        publishReplicationEventBackup(wanReplicationEvent.getServiceName(), eventObject);
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

    @Override
    public final void shutdown() {
        running = false;
        afterShutdown();
    }

    protected void afterShutdown() {
    }

    @Override
    public void pause() {
        paused = true;
        logger.info("Pausing WAN replication.");
    }

    @Override
    public void resume() {
        paused = false;
        if (resetCounters) {
            int backupCount = 0;
            int primaryCount = 0;
            for (IPartition partition : node.getPartitionService().getPartitions()) {
                int size = eventQueueContainer.size(partition.getPartitionId());
                if (!partition.isLocal()) {
                    backupCount += size;
                } else {
                    primaryCount += size;
                }
            }
            currentElementCount.set(primaryCount);
            currentBackupElementCount.set(backupCount);
            resetCounters = false;
        }
        logger.info("WAN replication resumed.");
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
        if (isThrowExceptionBehavior(queueFullBehavior)
                && currentElementCount.get() >= queueCapacity) {
            throw new WANReplicationQueueFullException(
                    String.format("WAN replication for target cluster %s is full. Queue capacity is %d",
                            targetGroupName, queueCapacity));
        }
    }

    /**
     * {@inheritDoc}
     * This implementation achieves ordering of keys in the same partition by
     * sending {@link EWRPutOperation} to the partition owner. The partition ID
     * is equal to the partition for the key contained in the {@code wanReplicationEvent}.
     *
     * @param wanReplicationEvent the WAN event to publish
     */
    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject replicationEventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        EWRPutOperation wanPutOperation =
                new EWRPutOperation(wanReplicationName, targetGroupName,
                        wanReplicationEvent,
                        replicationEventObject.getBackupCount());
        invokeOnPartition(wanReplicationEvent.getServiceName(), replicationEventObject.getKey(), wanPutOperation);
    }

    /**
     * Invokes the {@code operation} on the partition owner for the {@code key}
     *
     * @param serviceName the service name for this operation
     * @param key         the key defining the target partition for the operation
     * @param operation   the operation to be invoked
     */
    private void invokeOnPartition(String serviceName, Data key, Operation operation) {
        try {
            int partitionId = node.nodeEngine.getPartitionService().getPartitionId(key);
            node.nodeEngine.getOperationService()
                    .invokeOnPartition(serviceName, operation, partitionId).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private Object invokeOp(Operation operation) {
        try {

            Future future = node.nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, operation.getPartitionId())
                    .setResultDeserialized(false)
                    .invoke();
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private boolean isThrowExceptionBehavior(WANQueueFullBehavior queueFullBehavior) {
        return WANQueueFullBehavior.THROW_EXCEPTION == queueFullBehavior
                || (WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE == queueFullBehavior
                && !paused);
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (Operation op : this.liveOperations) {
            liveOperations.add(op.getCallerAddress(), op.getCallId());
        }
    }

    @Override
    public void publishSyncEvent(WanSyncEvent event) {
        try {
            syncRequests.put(event);
            liveOperations.add(event.getOp());
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    @Override
    public void clearQueues() {
        resetCounters = true;
        boolean alreadyPaused = paused;
        if (!alreadyPaused) {
            pause();
        }
        stagingQueue.clear();
        eventQueueContainer.clearQueues();
        resume();
    }

    /**
     * This class is responsible for polling the WAN replication queues and checking for {@link WanSyncEvent} and
     * filling the staging queue for events on specific entries.
     * If this publisher is {@link #pause()}'d it will backoff by sleeping the poller thread.
     */
    private class QueuePoller implements Runnable {

        private static final int MAX_SLEEP_MS = 2000;
        private static final int SLEEP_INTERVAL_MS = 200;
        private int emptyIterationCount;
        private WanSyncManager syncManager;

        QueuePoller(WanSyncManager syncManager) {
            this.syncManager = syncManager;
        }

        @Override
        public void run() {
            while (running) {

                WanSyncEvent syncEvent = syncRequests.poll();
                if (syncEvent != null) {
                    sync(syncEvent);
                    continue;
                }

                boolean offered = false;
                if (!paused) {
                    offered = tryWanReplicationQueues();
                    emptyIterationCount = 0;
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

        /** Poll a random replication event for every owned partition and offer it to the staging queue */
        private boolean tryWanReplicationQueues() {
            boolean offered = false;
            for (IPartition partition : node.getPartitionService().getPartitions()) {
                if (!partition.isLocal()) {
                    continue;
                }

                WanReplicationEvent event = eventQueueContainer.pollRandomWanEvent(partition.getPartitionId());
                if (event == null) {
                    continue;
                }
                offered = offerToStagingQueue(event);
            }
            return offered;
        }

        /** Offer a single event to the staging queue, blocking if necessary */
        private boolean offerToStagingQueue(WanReplicationEvent event) {
            boolean offered = false;
            while (!offered && running) {
                try {
                    stagingQueue.put(event);
                    offered = true;
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
            return offered;
        }

        /**
         * Syncs only {@link WanSyncEvent#getPartitionSet()} partitions or all partitions if the set is empty.
         *
         * @param syncEvent the partition sync event
         */
        private void sync(WanSyncEvent syncEvent) {
            syncManager.resetSyncedPartitionCount();
            WanSyncResult result = new WanSyncResult();
            try {
                Set<Integer> syncedPartitions = result.getSyncedPartitions();
                if (syncEvent.getPartitionSet().isEmpty()) {
                    for (IPartition partition : node.getPartitionService().getPartitions()) {
                        syncPartition(syncEvent, syncedPartitions, partition);
                    }
                } else {
                    for (Integer partitionIds : syncEvent.getPartitionSet()) {
                        IPartition partition = node.getPartitionService().getPartition(partitionIds);
                        syncPartition(syncEvent, syncedPartitions, partition);
                    }
                }
            } catch (Exception ex) {
                logger.warning(ex);
            } finally {
                sendResponse(syncEvent, result);
                liveOperations.remove(syncEvent.getOp());
            }
        }

        /** Syncs the given {@code partition} if this node is the owner and adds the partition to the {@code syncedPartitions} */
        private void syncPartition(WanSyncEvent syncEvent, Set<Integer> syncedPartitions, IPartition partition) {
            if (partition.isLocal()) {
                syncPartition(syncEvent, partition);
                syncedPartitions.add(partition.getPartitionId());
            }
        }

        /** Sends a response to the {@link WanSyncEvent#getOp()} */
        private void sendResponse(WanSyncEvent syncEvent, WanSyncResult result) {
            try {
                syncEvent.getOp().sendResponse(result);
            } catch (Exception ex) {
                logger.warning(ex);
            }
        }

        /** Syncs the {@code partition} for all maps or a specific map, depending on {@link WanSyncEvent#getType()} */
        private void syncPartition(WanSyncEvent syncEvent, IPartition partition) {
            int partitionEventCount = 0;
            counterMap.put(partition.getPartitionId(), new AtomicInteger());
            if (syncEvent.getType() == WanSyncType.ALL_MAPS) {
                for (String mapName : mapService.getMapServiceContext().getMapContainers().keySet()) {
                    partitionEventCount += syncPartitionForMap(mapName, partition);
                }
            } else {
                partitionEventCount += syncPartitionForMap(syncEvent.getName(), partition);
            }
            if (partitionEventCount == 0) {
                syncManager.incrementSyncedPartitionCount();
            }
        }

        /** Gets all map partition data and offers it to the staging queue, blocking until all entries have been offered */
        @SuppressWarnings("unchecked")
        private int syncPartitionForMap(String mapName, IPartition partition) {
            GetMapPartitionDataOperation op = new GetMapPartitionDataOperation(mapName);
            op.setPartitionId(partition.getPartitionId());
            Set<SimpleEntryView> set = (Set<SimpleEntryView>) invokeOp(op);
            counterMap.get(partition.getPartitionId()).addAndGet(set.size());

            for (SimpleEntryView simpleEntryView : set) {
                EnterpriseMapReplicationSync sync = new EnterpriseMapReplicationSync(
                        mapName, simpleEntryView, partition.getPartitionId());
                WanReplicationEvent event = new WanReplicationEvent(MapService.SERVICE_NAME, sync);
                offerToStagingQueue(event);
            }
            return set.size();
        }
    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
        final int partitionId = event.getPartitionId();
        final PartitionWanEventContainer partitionContainer = eventQueueContainer.getPublisherEventQueue(partitionId);

        if (partitionContainer == null) {
            return;
        }
        final int replicaIndex = event.getReplicaIndex();
        final PartitionWanEventQueueMap mapQueues = partitionContainer.getMapEventQueueMapByBackupCount(replicaIndex);
        final PartitionWanEventQueueMap cacheQueues = partitionContainer.getCacheEventQueueMapByBackupCount(replicaIndex);
        for (String mapName : mapQueues.keySet()) {
            namespaces.add(MapService.getObjectNamespace(mapName));
        }
        for (String cacheName : cacheQueues.keySet()) {
            namespaces.add(CacheService.getObjectNamespace(cacheName));
        }
    }

    @Override
    public void collectReplicationData(String wanReplicationName,
                                       PartitionReplicationEvent event,
                                       Collection<ServiceNamespace> namespaces,
                                       EWRMigrationContainer migrationDataContainer) {
        final PartitionWanEventContainer partitionContainer = eventQueueContainer.getPublisherEventQueue(event.getPartitionId());
        if (partitionContainer != null) {
            final PartitionWanEventQueueMap mapQueues = collectNamespaces(
                    partitionContainer.getMapEventQueueMapByBackupCount(event.getReplicaIndex()),
                    MapService.SERVICE_NAME, namespaces);

            final PartitionWanEventQueueMap cacheQueues = collectNamespaces(
                    partitionContainer.getCacheEventQueueMapByBackupCount(event.getReplicaIndex()),
                    CacheService.SERVICE_NAME, namespaces);

            if (!isNullOrEmpty(mapQueues)) {
                migrationDataContainer.addMapEventQueueMap(wanReplicationName, targetGroupName, mapQueues);
            }
            if (!isNullOrEmpty(cacheQueues)) {
                migrationDataContainer.addCacheEventQueueMap(wanReplicationName, targetGroupName, cacheQueues);
            }
        }
    }

    /**
     * Filter and collect {@link WanReplicationEventQueue}s that contain events matching the
     * {@code serviceName} and one of the provided {@code namespaces}. The namespaces must be
     * of the type {@link ObjectNamespace}.
     *
     * @param queues      WAN event queue map of a partition and a specific service (map/cache)
     * @param serviceName the service name for which we are collecting WAN queues
     * @param namespaces  the collection of {@link ObjectNamespace}s for which we are collecting WAN queues
     * @return the filtered map from distributed object name to WAN queue
     */
    private PartitionWanEventQueueMap collectNamespaces(PartitionWanEventQueueMap queues, String serviceName,
                                                        Collection<ServiceNamespace> namespaces) {
        if (queues.isEmpty()) {
            return null;
        }

        final PartitionWanEventQueueMap filteredQueues = new PartitionWanEventQueueMap();
        for (ServiceNamespace namespace : namespaces) {
            if (!serviceName.equals(namespace.getServiceName())) {
                continue;
            }

            final ObjectNamespace ns = (ObjectNamespace) namespace;
            final WanReplicationEventQueue q = queues.get(ns.getObjectName());
            if (q != null) {
                filteredQueues.put(ns.getObjectName(), q);
            }
        }
        return filteredQueues;
    }

    /**
     * Returns the count for the number of events currently in the WAN
     * map/cache partition queues for partitions owned by this node.
     */
    public int getCurrentElementCount() {
        return currentElementCount.get();
    }

    public int getCurrentBackupElementCount() {
        return currentBackupElementCount.get();
    }
}
