package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.wan.CacheReplicationObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.DistributedObjectIdentifier;
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
import com.hazelcast.enterprise.wan.operation.RemoveWanEventBackupsOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.Versions;
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
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.util.Clock;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static java.util.Collections.unmodifiableMap;

/**
 * Abstract WAN event publisher implementation.
 * This implementation prepares the WAN events from the WAN queues and
 * from WAN sync events for sending to the WAN endpoints.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractWanPublisher implements WanReplicationPublisher,
        WanReplicationEndpoint, WanEventQueueMigrationListener {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

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
    protected WanPublisherSyncSupport syncSupport;

    private EnterpriseWanReplicationService wanService;
    private WanQueueMigrationSupport wanQueueMigrationSupport;
    private final LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        this.configurationContext = new WanConfigurationContext(publisherConfig);
        this.node = node;
        this.wanReplicationName = wanReplicationConfig.getName();
        this.wanPublisherId = EnterpriseWanReplicationService.getPublisherIdOrGroupName(publisherConfig);
        this.logger = node.getLogger(getClass());
        this.queueCapacity = publisherConfig.getQueueCapacity();
        this.localGroupName = node.getNodeEngine().getConfig().getGroupConfig().getName();
        this.eventQueueContainer = new PollSynchronizerPublisherQueueContainer(node);
        this.queueFullBehavior = publisherConfig.getQueueFullBehavior();
        this.wanService = (EnterpriseWanReplicationService) node.getNodeEngine().getWanReplicationService();

        final DistributedServiceWanEventCounters mapCounters = wanService.getSentEventCounters(
                wanReplicationName, wanPublisherId, MapService.SERVICE_NAME);
        final DistributedServiceWanEventCounters cacheCounters = wanService.getSentEventCounters(
                wanReplicationName, wanPublisherId, ICacheService.SERVICE_NAME);
        this.localWanPublisherStats.setSentMapEventCounter(unmodifiableMap(mapCounters.getEventCounterMap()));
        this.localWanPublisherStats.setSentCacheEventCounter(unmodifiableMap(cacheCounters.getEventCounterMap()));
        this.wanQueueMigrationSupport = new WanQueueMigrationSupport(eventQueueContainer, wanCounter);
        this.state = publisherConfig.getInitialPublisherState();
        this.syncSupport = createWanSyncSupport();
    }

    /**
     * Returns the partition ID for the partition owning the {@code key}
     */
    protected int getPartitionId(Object key) {
        return node.getNodeEngine().getPartitionService().getPartitionId(key);
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
     * Publishes the {@code eventObject} onto the WAN replication queues. WAN
     * sync events should not be published using this method.
     *
     * @param serviceName the name of the service to which this event belongs
     * @param eventObject the WAN event
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
     * Finalizes WAN replication by updating the statistics and removing WAN
     * events on backup replicas.
     *
     * @param eventCollections the collections containing the replicated WAN events
     */
    void finalizeWanEventReplication(Collection<WanReplicationEvent>... eventCollections) {
        // RU_COMPAT_3_11
        if (node.getClusterService().getClusterVersion().isLessThan(Versions.V3_12)) {
            for (Collection<WanReplicationEvent> events : eventCollections) {
                for (WanReplicationEvent event : events) {
                    removeReplicationEvent(event);
                }
            }
            return;
        }

        Map<Integer, Map<DistributedObjectIdentifier, Integer>> eventCounts
                = updateStatsAndCountEvents(eventCollections);
        if (eventCounts.isEmpty()) {
            return;
        }

        List<InternalCompletableFuture> futures = invokeBackupRemovalOperations(eventCounts);
        try {
            for (InternalCompletableFuture future : futures) {
                future.get();
            }
        } catch (Exception ex) {
            logger.warning("Exception occurred while removing wan backups", ex);
        }
    }

    private List<InternalCompletableFuture> invokeBackupRemovalOperations(
            Map<Integer, Map<DistributedObjectIdentifier, Integer>> eventCounts) {
        List<InternalCompletableFuture> futures = new ArrayList<InternalCompletableFuture>(eventCounts.size());

        for (Entry<Integer, Map<DistributedObjectIdentifier, Integer>> partitionEntry : eventCounts.entrySet()) {
            Integer partitionId = partitionEntry.getKey();
            Map<DistributedObjectIdentifier, Integer> partitionEventCounts = partitionEntry.getValue();

            int backupCount = 0;
            for (DistributedObjectIdentifier id : partitionEventCounts.keySet()) {
                backupCount = Math.max(backupCount, id.getTotalBackupCount());
            }

            int maxAllowedBackupCount = node.getPartitionService().getMaxAllowedBackupCount();
            for (int i = 0; i < backupCount && i < maxAllowedBackupCount; i++) {
                RemoveWanEventBackupsOperation op = new RemoveWanEventBackupsOperation(
                        wanReplicationName, wanPublisherId, partitionEventCounts);
                InternalCompletableFuture<Object> future =
                        node.getNodeEngine()
                            .getOperationService()
                            .createInvocationBuilder(EnterpriseWanReplicationService.SERVICE_NAME, op, partitionId)
                            .setResultDeserialized(false)
                            .setReplicaIndex(i + 1)
                            .invoke();
                futures.add(future);
            }
        }
        return futures;
    }

    /**
     * Removes a {@code count} number of events from a WAN replication queue
     * belonging to the provided service, object and partition.
     *
     * @param serviceName the service name for the WAN replication queue
     * @param objectName  the object name for the WAN replication queue
     * @param partitionId the partition ID of the WAN replication queue
     * @param count       the number of events to remove from the queue
     */
    public void removeBackups(String serviceName, String objectName, int partitionId, int count) {
        boolean isMapService = MapService.SERVICE_NAME.equals(serviceName);
        if (!isMapService && !CacheService.SERVICE_NAME.equals(serviceName)) {
            logger.warning("Unexpected replication event service name: " + serviceName);
            return;
        }

        for (int i = 0; i < count; i++) {
            WanReplicationEvent event = isMapService
                    ? eventQueueContainer.pollMapWanEvent(objectName, partitionId)
                    : eventQueueContainer.pollCacheWanEvent(objectName, partitionId);
            if (event != null) {
                wanCounter.decrementBackupElementCounter();
            }
        }
    }

    /**
     * Updates the statistics for the provided replicated WAN events and counts
     * them by partition ID and distributed object.
     *
     * @param eventCollections collections of replicated WAN events
     * @return the number of replicated events, per partition and distributed object
     */
    private Map<Integer, Map<DistributedObjectIdentifier, Integer>> updateStatsAndCountEvents(
            Collection<WanReplicationEvent>... eventCollections) {
        Map<Integer, Map<DistributedObjectIdentifier, Integer>> eventCounts
                = new HashMap<Integer, Map<DistributedObjectIdentifier, Integer>>();

        for (Collection<WanReplicationEvent> events : eventCollections) {
            for (WanReplicationEvent event : events) {
                ReplicationEventObject eventObject = event.getEventObject();
                if (eventObject instanceof EnterpriseMapReplicationSync
                        || eventObject instanceof EnterpriseMapReplicationMerkleTreeNode) {
                    syncSupport.removeReplicationEvent((EnterpriseMapReplicationObject) eventObject);
                    continue;
                }
                updateStats(event);

                int partitionId = getPartitionId(eventObject.getKey());
                if (!eventCounts.containsKey(partitionId)) {
                    eventCounts.put(partitionId, new HashMap<DistributedObjectIdentifier, Integer>());
                }

                Map<DistributedObjectIdentifier, Integer> partitionEventCounts = eventCounts.get(partitionId);

                DistributedObjectIdentifier id = getDistributedObjectIdentifier(eventObject);

                Integer eventCount = partitionEventCounts.get(id);
                partitionEventCounts.put(id, eventCount != null ? eventCount + 1 : 1);
            }
        }
        return eventCounts;
    }

    /**
     * Returns the identifier for the distributed object for which the provided
     * event is pertained to.
     *
     * @param eventObject the WAN replication event
     * @return the identifier for the distributed object on which the event happened
     */
    private DistributedObjectIdentifier getDistributedObjectIdentifier(ReplicationEventObject eventObject) {
        DistributedObjectIdentifier id = null;
        if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapEvent = (EnterpriseMapReplicationObject) eventObject;
            id = new DistributedObjectIdentifier(
                    MapService.SERVICE_NAME, mapEvent.getMapName(), mapEvent.getBackupCount());
        } else if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheEvent = (CacheReplicationObject) eventObject;
            id = new DistributedObjectIdentifier(
                    CacheService.SERVICE_NAME, cacheEvent.getNameWithPrefix(), cacheEvent.getBackupCount());
        } else {
            logger.warning("Unexpected replication event object type" + eventObject.getClass().getName());
        }
        return id;
    }


    /**
     * Updates the WAN statistics (remaining map sync events, synced partitions,
     * queued event count, latencies, sent events) and removes the backup events
     * on the replicas.
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
        final OperationService operationService = node.getNodeEngine().getOperationService();
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
        localWanPublisherStats.setLastSyncStats(syncSupport.getLastSyncStats());
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
            int partitionId = node.getNodeEngine().getPartitionService().getPartitionId(key);
            node.getNodeEngine().getOperationService()
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
    public void publishSyncEvent(WanSyncEvent event) {
        publishAntiEntropyEvent(event);
    }

    /**
     * Publishes a WAN anti-entropy event. This method may also process the
     * event or trigger processing.
     *
     * @param event the WAN anti-entropy event
     */
    public abstract void publishAntiEntropyEvent(WanAntiEntropyEvent event);

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
        int totalDrained = 0;

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
        final PartitionWanEventContainer partitionContainer = eventQueueContainer.getEventQueue(partitionId);

        if (partitionContainer == null) {
            return;
        }
        final int replicaIndex = event.getReplicaIndex();
        final PartitionWanEventQueueMap mapQueues = partitionContainer.getMapEventQueueMapByBackupCount(replicaIndex);
        final PartitionWanEventQueueMap cacheQueues = partitionContainer.getCacheEventQueueMapByBackupCount(replicaIndex);
        // Use ConcurrentHashMap#keys() instead of ConcurrentHashMap#keySet(). It is not byte-code compatible with JDK7
        // Return type of keySet() method does not exist in Java 7
        for (Enumeration<String> mapNames = mapQueues.keys(); mapNames.hasMoreElements();) {
            namespaces.add(MapService.getObjectNamespace(mapNames.nextElement()));
        }
        for (Enumeration<String> cacheNames = cacheQueues.keys(); cacheNames.hasMoreElements();) {
            namespaces.add(CacheService.getObjectNamespace(cacheNames.nextElement()));
        }
    }

    @Override
    public void collectReplicationData(String wanReplicationName,
                                       PartitionReplicationEvent event,
                                       Collection<ServiceNamespace> namespaces,
                                       EWRMigrationContainer migrationDataContainer) {
        final PartitionWanEventContainer partitionContainer = eventQueueContainer.getEventQueue(event.getPartitionId());
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
     * Creates the instance implementing WAN sync features. This method must
     * not return {@code null}.
     */
    protected abstract WanPublisherSyncSupport createWanSyncSupport();

    /**
     * Returns the WAN configuration context for the configuration used to
     * instantiate this publisher.
     */
    WanConfigurationContext getConfigurationContext() {
        return configurationContext;
    }
}
