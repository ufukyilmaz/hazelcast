package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PartitionWanEventContainer;
import com.hazelcast.enterprise.wan.PartitionWanEventQueueMap;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanEventQueueMigrationListener;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
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
import com.hazelcast.util.QueueUtil;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Abstract WAN event publisher implementation.
 * This implementation prepares the WAN events from the WAN queues and
 * from WAN sync events for sending to the WAN endpoints.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractWanPublisher implements WanReplicationPublisher,
        WanReplicationEndpoint, LiveOperationsTracker, WanEventQueueMigrationListener {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);
    private static final int DEFAULT_STAGING_QUEUE_SIZE = 100;

    protected final WanElementCounter wanCounter = new WanElementCounter();

    protected volatile WanPublisherState state;
    protected volatile boolean running = true;
    protected volatile long lastQueueFullLogTimeMs;

    protected int queueCapacity;
    protected int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;
    protected Node node;
    protected ILogger logger;
    protected String localGroupName;
    protected String wanPublisherId;
    protected String wanReplicationName;
    protected WANQueueFullBehavior queueFullBehavior;
    protected WanConfigurationContext configurationContext;
    protected PollSynchronizerPublisherQueueContainer eventQueueContainer;
    /**
     * Queue for multiplexed map/cache partition WAN events that are about to
     * be transmitted to the target cluster
     */
    protected BlockingQueue<WanReplicationEvent> stagingQueue;

    private EnterpriseWanReplicationService wanService;
    private WanQueueMigrationSupport wanQueueMigrationSupport;
    private WanPublisherSyncSupport syncSupport;
    private final LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();
    private final Set<Operation> liveOperations = newSetFromMap(new ConcurrentHashMap<Operation, Boolean>());
    private final BlockingQueue<WanAntiEntropyEvent> antiEntropyEvents
            = new ArrayBlockingQueue<WanAntiEntropyEvent>(DEFAULT_STAGING_QUEUE_SIZE);

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        this.configurationContext = new WanConfigurationContext(publisherConfig);
        this.node = node;
        this.wanReplicationName = wanReplicationConfig.getName();
        this.wanPublisherId = EnterpriseWanReplicationService.getPublisherIdOrGroupName(publisherConfig);
        this.logger = node.getLogger(getClass());
        this.queueCapacity = publisherConfig.getQueueCapacity();
        this.localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();
        this.eventQueueContainer = new PollSynchronizerPublisherQueueContainer(node);
        this.stagingQueue = new ArrayBlockingQueue<WanReplicationEvent>(getStagingQueueSize());
        this.queueFullBehavior = publisherConfig.getQueueFullBehavior();
        this.wanService = (EnterpriseWanReplicationService) node.nodeEngine.getWanReplicationService();

        final DistributedServiceWanEventCounters mapCounters = wanService.getSentEventCounters(
                wanReplicationName, wanPublisherId, MapService.SERVICE_NAME);
        final DistributedServiceWanEventCounters cacheCounters = wanService.getSentEventCounters(
                wanReplicationName, wanPublisherId, ICacheService.SERVICE_NAME);
        this.localWanPublisherStats.setSentMapEventCounter(unmodifiableMap(mapCounters.getEventCounterMap()));
        this.localWanPublisherStats.setSentCacheEventCounter(unmodifiableMap(cacheCounters.getEventCounterMap()));
        this.wanQueueMigrationSupport = new WanQueueMigrationSupport(eventQueueContainer, wanCounter);
        this.state = publisherConfig.getInitialPublisherState();
        this.syncSupport = createWanSyncSupport();

        node.nodeEngine.getExecutionService().execute("hz:wan:poller", new QueuePoller());
    }

    /**
     * Returns the capacity of the WAN staging queue. The staging queue is a
     * queue for multiplexed map/cache partition WAN events that are about to
     * be transmitted to the target cluster.
     *
     * @see AbstractWanPublisher#stagingQueue
     */
    public int getStagingQueueSize() {
        return DEFAULT_STAGING_QUEUE_SIZE;
    }

    /**
     * Returns the partition ID for the partition owning the {@code key}
     */
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

    /**
     * Publish the {@code eventObject} WAN replication event backup. The event
     * may be dropped if queue capacity has been reached.
     * The event will be published to the target endpoints of this publisher
     * if the publisher has not already processed this event.
     *
     * @param serviceName the service publishing the event
     * @param eventObject the replication backup event
     * @param backupEvent if this is an event of a backup entry
     */
    private void publishReplicationEventInternal(String serviceName,
                                                 EnterpriseReplicationEventObject eventObject,
                                                 boolean backupEvent) {
        if (!state.isEnqueueNewEvents()) {
            return;
        }

        if (isEventDroppingNeeded(backupEvent)) {
            if (!backupEvent) {
                wanService.getSentEventCounters(wanReplicationName, wanPublisherId, serviceName)
                          .incrementDropped(eventObject.getObjectName());
            }
            return;
        }

        if (eventObject.getGroupNames()
                       .contains(configurationContext.getGroupName())) {
            return;
        }

        eventObject.getGroupNames().add(localGroupName);

        boolean eventPublished = publishEventInternal(serviceName, eventObject);
        if (eventPublished) {
            wanCounter.incrementCounters(backupEvent);
        }
    }

    /**
     * Publishes the {@code eventObject}
     *
     * @param serviceName
     * @param eventObject
     * @return {@code true} if the event has been published, otherwise returns {@code false}
     */
    private boolean publishEventInternal(String serviceName, ReplicationEventObject eventObject) {
        assert !(eventObject instanceof EnterpriseMapReplicationMerkleTreeNode)
                : "Merkle tree sync objects should not be published";
        assert !(eventObject instanceof EnterpriseMapReplicationSync)
                : "Sync objects should not be published";

        if (eventObject instanceof EnterpriseMapReplicationObject) {
            String mapName = ((EnterpriseMapReplicationObject) eventObject).getMapName();
            int partitionId = getPartitionId(eventObject.getKey());
            return eventQueueContainer.publishMapWanEvent(mapName, partitionId,
                    wrapReplicationEvent(serviceName, eventObject));
        }

        if (eventObject instanceof CacheReplicationObject) {
            String cacheName = ((CacheReplicationObject) eventObject).getNameWithPrefix();
            int partitionId = getPartitionId(eventObject.getKey());
            return eventQueueContainer.publishCacheWanEvent(cacheName, partitionId,
                    wrapReplicationEvent(serviceName, eventObject));
        }

        logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());

        return false;
    }

    /**
     * Creates a new WanReplicationEvent to wrap a ReplicationEventObject in
     * order to send it over wire.
     */
    private WanReplicationEvent wrapReplicationEvent(String serviceName,
                                                     ReplicationEventObject eventObject) {
        return new WanReplicationEvent(serviceName, eventObject);
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
        final DistributedServiceWanEventCounters counters
                = wanService.getSentEventCounters(wanReplicationName, wanPublisherId, serviceName);
        eventObject.incrementEventCount(counters);
    }

    /**
     * Determine if queue capacity has been reached and if the event should be
     * dropped
     */
    private boolean isEventDroppingNeeded(boolean isBackup) {
        if (isBackup) {
            return wanCounter.getBackupElementCount() >= queueCapacity;
        }

        if (wanCounter.getPrimaryElementCount() >= queueCapacity
                && queueFullBehavior != WANQueueFullBehavior.THROW_EXCEPTION) {
            long curTime = System.currentTimeMillis();
            if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                lastQueueFullLogTimeMs = curTime;
                logger.severe("Wan replication event queue is full. Dropping events. Queue size : " + wanCounter
                        .getPrimaryElementCount());
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
        if (wanReplicationEvent.getEventObject() instanceof EnterpriseMapReplicationSync
                || wanReplicationEvent.getEventObject() instanceof EnterpriseMapReplicationMerkleTreeNode) {
            syncSupport.removeReplicationEvent(
                    (EnterpriseMapReplicationObject) wanReplicationEvent.getEventObject());
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
                        = new EWRRemoveBackupOperation(wanReplicationName, wanPublisherId, wanReplicationEventData);
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

    /**
     * Update the published count and latency between event creation and publication
     */
    private void updateStats(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject eventObject
                = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        long latency = Clock.currentTimeMillis() - eventObject.getCreationTime();
        localWanPublisherStats.incrementPublishedEventCount(latency < 0 ? 0 : latency);
    }

    @Override
    public void removeBackup(WanReplicationEvent wanReplicationEvent) {
        ReplicationEventObject eventObject = wanReplicationEvent.getEventObject();

        if (eventObject instanceof EnterpriseMapReplicationObject) {
            String mapName = ((EnterpriseMapReplicationObject) eventObject).getMapName();
            int partitionId = getPartitionId(eventObject.getKey());
            WanReplicationEvent event = eventQueueContainer.pollMapWanEvent(mapName, partitionId);
            if (event != null) {
                wanCounter.decrementBackupElementCounter();
            }
            return;
        }

        if (eventObject instanceof CacheReplicationObject) {
            String cacheName = ((CacheReplicationObject) eventObject).getNameWithPrefix();
            int partitionId = getPartitionId(eventObject.getKey());
            WanReplicationEvent event = eventQueueContainer.pollCacheWanEvent(cacheName, partitionId);
            if (event != null) {
                wanCounter.decrementBackupElementCounter();
            }
            return;
        }

        logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
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
        // nothing to do here, this method is not invoked
    }

    @Override
    public void addCacheQueue(String name, int partitionId, WanReplicationEventQueue eventQueue) {
        // nothing to do here, this method is not invoked
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
        state = WanPublisherState.PAUSED;
        logger.info("Paused WAN replication " + wanReplicationName + ",publisherId: " + wanPublisherId);
    }

    @Override
    public void stop() {
        state = WanPublisherState.STOPPED;
        logger.info("Stopped WAN replication " + wanReplicationName + ",publisherId: " + wanPublisherId);
    }

    @Override
    public void resume() {
        state = WanPublisherState.REPLICATING;
        logger.info("Resumed WAN replication " + wanReplicationName + ",publisherId: " + wanPublisherId);
    }

    @Override
    public LocalWanPublisherStats getStats() {
        localWanPublisherStats.setState(state);
        localWanPublisherStats.setConnected(isConnected());
        localWanPublisherStats.setOutboundQueueSize(wanCounter.getPrimaryElementCount());
        localWanPublisherStats.setLastConsistencyCheckResults(syncSupport.getLastConsistencyCheckResults());
        return localWanPublisherStats;
    }

    /**
     * Tests is there is at least one alive connection to a configured
     * target cluster member over WAN.
     *
     * @return {@code true} if there is at least one alive connection, {@code false} otherwise
     */
    public abstract boolean isConnected();

    @Override
    public void checkWanReplicationQueues() {
        if (isThrowExceptionBehavior(queueFullBehavior) && wanCounter.getPrimaryElementCount() >= queueCapacity) {
            throw new WANReplicationQueueFullException(
                    String.format("WAN replication for WAN publisher %s is full. Queue capacity is %d",
                            wanPublisherId, queueCapacity));
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
                new EWRPutOperation(wanReplicationName, wanPublisherId,
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

    private boolean isThrowExceptionBehavior(WANQueueFullBehavior queueFullBehavior) {
        return WANQueueFullBehavior.THROW_EXCEPTION == queueFullBehavior
                || (WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE == queueFullBehavior
                && state.isReplicateEnqueuedEvents());
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (Operation op : this.liveOperations) {
            liveOperations.add(op.getCallerAddress(), op.getCallId());
        }
    }

    @Override
    public void publishSyncEvent(WanSyncEvent event) {
        publishAntiEntropyEvent(event);
    }

    /**
     * Publishes an anti-entropy event.
     * This method does not wait for the event processing to complete.
     *
     * @param event the WAN anti-entropy event
     */
    public void publishAntiEntropyEvent(WanAntiEntropyEvent event) {
        try {
            antiEntropyEvents.put(event);
            liveOperations.add(event.getOp());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw rethrow(e);
        }
    }

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map name
     */
    public void destroyMapData(String mapName) {
        syncSupport.destroyMapData(mapName);
    }

    @Override
    public void clearQueues() {
        boolean isReplicating = state.isReplicateEnqueuedEvents();
        if (isReplicating) {
            pause();
        }

        clearQueuesInternal();

        if (isReplicating) {
            resume();
        }
    }

    private void clearQueuesInternal() {
        final int drainedFromStagingQueue = QueueUtil.drainQueue(stagingQueue);
        // the elements in the staging queue were polled from queues
        // belong to owned partitions, therefore we should decrement the
        // primary counter
        wanCounter.decrementPrimaryElementCounter(drainedFromStagingQueue);
        int totalDrained = drainedFromStagingQueue;

        Map<Integer, Integer> drainedPerPartition = eventQueueContainer.drainQueues();
        for (Map.Entry<Integer, Integer> entry : drainedPerPartition.entrySet()) {
            Integer partitionId = entry.getKey();
            int drainedElements = entry.getValue();
            totalDrained += drainedElements;

            InternalPartition partition = node.getPartitionService().getPartition(partitionId);

            if (partition.isLocal()) {
                wanCounter.decrementPrimaryElementCounter(drainedElements);
            } else {
                wanCounter.decrementBackupElementCounter(drainedElements);
            }
        }

        logger.info("Cleared " + totalDrained + " elements from the WAN queues. Current element counts:"
                + " primary=" + wanCounter.getPrimaryElementCount()
                + " backup=" + wanCounter.getBackupElementCount());
    }

    @Override
    public void onMigrationStart(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
        wanQueueMigrationSupport.onMigrationStart(partitionId, currentReplicaIndex, newReplicaIndex);
    }

    @Override
    public void onMigrationCommit(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
        wanQueueMigrationSupport.onMigrationCommit(partitionId, currentReplicaIndex, newReplicaIndex);
    }

    @Override
    public void onMigrationRollback(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
        wanQueueMigrationSupport.onMigrationRollback(partitionId, currentReplicaIndex, newReplicaIndex);
    }

    @Override
    public void onWanQueueClearedDuringMigration(int partitionId, int currentReplicaIndex, int clearedQueueDepth) {
        wanQueueMigrationSupport.onWanQueueClearedDuringMigration(partitionId, currentReplicaIndex, clearedQueueDepth);
    }

    /**
     * Returns the count for the number of events currently in the WAN
     * map/cache partition queues for partitions owned by this node.
     */
    public int getCurrentElementCount() {
        return wanCounter.getPrimaryElementCount();
    }

    /**
     * Returns the count for the number of events currently in the WAN
     * map/cache partition queues for partitions for which this node is
     * a backup replica.
     */
    public int getCurrentBackupElementCount() {
        return wanCounter.getBackupElementCount();
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
                migrationDataContainer.addMapEventQueueMap(wanReplicationName, wanPublisherId, mapQueues);
            }
            if (!isNullOrEmpty(cacheQueues)) {
                migrationDataContainer.addCacheEventQueueMap(wanReplicationName, wanPublisherId, cacheQueues);
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
     * Offer a single event to the staging queue, blocking if necessary
     *
     * @return {@code true} if the event was accepted by the queue. This will
     * always be {@code true} unless the WAN publisher is shutting down.
     */
    boolean offerToStagingQueue(WanReplicationEvent event) {
        while (running) {
            try {
                stagingQueue.put(event);
                return true;
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
        }
        return false;
    }

    /**
     * Sends a response to the {@link WanAntiEntropyEvent#getOp()}
     */
    private void sendResponse(WanAntiEntropyEvent event,
                              WanAntiEntropyEventResult result) {
        try {
            event.getOp().sendResponse(result);
        } catch (Exception ex) {
            logger.warning(ex);
        }
    }

    /**
     * Creates a support instance for processing WAN merkle tree anti-entropy
     * events. This method may return {@code null} if merkle tree WAN sync is
     * not supported.
     */
    protected WanPublisherSyncSupport createWanSyncSupport() {
        return new WanPublisherFullSyncSupport(node, this);
    }

    /**
     * This runnable is responsible for multiplexing the WAN events from the
     * WAN replication queues with WAN events triggered by a
     * {@link WanSyncEvent}. It then fills the staging queue with WAN events
     * on specific entries.
     * If this publisher is {@link #pause()}'d or if there are no events in the
     * WAN queues, it will backoff by sleeping the poller thread up to
     * {@link QueuePoller#MAX_SLEEP_MS}
     */
    private class QueuePoller implements Runnable {
        /**
         * The maximum time to sleep the queue poller thread if there are no
         * events or the publisher is paused
         */
        private static final int MAX_SLEEP_MS = 2000;
        private static final int SLEEP_INTERVAL_MS = 200;
        private int emptyIterationCount;

        @Override
        public void run() {
            while (running) {
                if (tryProcessAntiEntropyEvent()) {
                    continue;
                }

                boolean offered = false;
                if (state.isReplicateEnqueuedEvents()) {
                    offered = tryWanReplicationQueues();
                    emptyIterationCount = 0;
                }

                if (!offered && running) {
                    int sleepMsSuggestion = ++emptyIterationCount * SLEEP_INTERVAL_MS;
                    try {
                        TimeUnit.MILLISECONDS.sleep(Math.min(sleepMsSuggestion, MAX_SLEEP_MS));
                    } catch (InterruptedException ignored) {
                        EmptyStatement.ignore(ignored);
                    }
                }
            }
        }

        /**
         * Checks if there is an anti-entropy request and processes the request.
         *
         * @return {@code true} if the method processed a WAN anti-entropy event
         */
        private boolean tryProcessAntiEntropyEvent() {
            WanAntiEntropyEvent event = antiEntropyEvents.poll();
            if (event != null) {
                final WanAntiEntropyEventResult result = new WanAntiEntropyEventResult();
                try {
                    if (event instanceof WanSyncEvent) {
                        syncSupport.processEvent((WanSyncEvent) event, result);
                        return true;
                    }
                    if (event instanceof WanConsistencyCheckEvent) {
                        syncSupport.processEvent((WanConsistencyCheckEvent) event, result);
                        return true;
                    }

                    logger.info("Ignoring unknown WAN anti-entropy event " + event);
                } catch (Exception ex) {
                    logger.warning("WAN anti-entropy event processing failed", ex);
                } finally {
                    sendResponse(event, result);
                    liveOperations.remove(event.getOp());
                }
            }
            return false;
        }

        /**
         * Polls a random replication event for every owned partition and adds it
         * to the staging queue.
         * If there is an event polled from a partition, it will be added to the
         * staging queue, blocking if necessary.
         *
         * @return {@code true} if any event was offered to the staging queue. This
         * may be {@code false} if there were no events or if the publisher is
         * shutting down
         * @see #stagingQueue
         */
        private boolean tryWanReplicationQueues() {
            boolean offered = false;
            for (IPartition partition : node.getPartitionService().getPartitions()) {
                if (!partition.isLocal()) {
                    continue;
                }

                WanReplicationEvent event = eventQueueContainer.pollRandomWanEvent(partition.getPartitionId());

                if (event != null) {
                    offered |= offerToStagingQueue(event);
                }
            }
            return offered;
        }
    }
}
