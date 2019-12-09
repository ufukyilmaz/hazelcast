package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.wan.WanCacheEvent;
import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.wan.impl.DistributedObjectIdentifier;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.PartitionWanEventQueueMap;
import com.hazelcast.enterprise.wan.impl.WanEventMigrationContainer;
import com.hazelcast.enterprise.wan.impl.WanEventQueue;
import com.hazelcast.enterprise.wan.impl.operation.WanPutOperation;
import com.hazelcast.enterprise.wan.impl.operation.RemoveWanEventBackupsOperation;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.WanEnterpriseMapMerkleTreeNode;
import com.hazelcast.map.impl.wan.WanEnterpriseMapEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapSyncEvent;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanMigrationAwarePublisher;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.WanQueueFullException;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.InternalWanPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;
import static com.hazelcast.wan.impl.WanReplicationServiceImpl.getWanPublisherId;
import static java.util.Collections.unmodifiableMap;

/**
 * Abstract WAN event publisher implementation.
 * This implementation prepares the WAN events from the WAN queues and
 * from WAN sync events for sending to the WAN endpoints.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractWanPublisher implements
        InternalWanPublisher,
        WanMigrationAwarePublisher<WanEventMigrationContainer>, HazelcastInstanceAware {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

    protected final WanElementCounter wanCounter = new WanElementCounter();

    protected volatile WanPublisherState state;
    protected volatile boolean running = true;
    protected volatile long lastQueueFullLogTimeMs;

    protected int queueCapacity;
    protected int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;
    protected Node node;
    protected ILogger logger;
    protected String localClusterName;
    protected String wanPublisherId;
    protected String wanReplicationName;
    protected WanQueueFullBehavior queueFullBehavior;
    protected WanConfigurationContext configurationContext;
    protected PollSynchronizerPublisherQueueContainer eventQueueContainer;
    protected WanPublisherSyncSupport syncSupport;

    private EnterpriseWanReplicationService wanService;
    private WanQueueMigrationSupport wanQueueMigrationSupport;
    private final LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.node = ((HazelcastInstanceImpl) hazelcastInstance).node;
    }

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig config) {
        WanBatchPublisherConfig publisherConfig = (WanBatchPublisherConfig) config;
        this.configurationContext = new WanConfigurationContext(publisherConfig);
        this.wanReplicationName = wanReplicationConfig.getName();
        this.wanPublisherId = getWanPublisherId(publisherConfig);
        this.logger = node.getLogger(getClass());
        this.queueCapacity = publisherConfig.getQueueCapacity();
        this.localClusterName = node.getNodeEngine().getConfig().getClusterName();
        this.eventQueueContainer = new PollSynchronizerPublisherQueueContainer(node);
        this.queueFullBehavior = publisherConfig.getQueueFullBehavior();
        this.wanService = (EnterpriseWanReplicationService) node.getNodeEngine().getWanReplicationService();

        final WanEventCounters mapCounters = wanService.getSentEventCounters(
                wanReplicationName, wanPublisherId, MapService.SERVICE_NAME);
        final WanEventCounters cacheCounters = wanService.getSentEventCounters(
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

    /**
     * {@inheritDoc}
     * The event may be dropped if queue capacity has been reached.
     *
     * @param eventObject the replication event
     */
    @Override
    public void publishReplicationEvent(WanEvent eventObject) {
        publishReplicationEventInternal((InternalWanEvent) eventObject, false);
    }

    /**
     * {@inheritDoc}
     * The event may be dropped if queue capacity has been reached.
     *
     * @param eventObject the replication backup event
     */
    @Override
    public void publishReplicationEventBackup(WanEvent eventObject) {
        publishReplicationEventInternal((InternalWanEvent) eventObject, true);
    }

    /**
     * Publish the {@code eventObject} WAN replication event backup. The event
     * may be dropped if queue capacity has been reached.
     * The event will be published to the target endpoints of this publisher
     * if the publisher has not already processed this event.
     *
     * @param eventObject the replication backup event
     * @param backupEvent if this is an event of a backup entry
     */
    private void publishReplicationEventInternal(InternalWanEvent eventObject,
                                                 boolean backupEvent) {
        if (!state.isEnqueueNewEvents()) {
            return;
        }

        if (isEventDroppingNeeded(backupEvent)) {
            if (!backupEvent) {
                wanService.getSentEventCounters(wanReplicationName, wanPublisherId, eventObject.getServiceName())
                          .incrementDropped(eventObject.getObjectName());
            }
            return;
        }

        if (eventObject.getClusterNames()
                       .contains(configurationContext.getClusterName())) {
            return;
        }

        eventObject.getClusterNames().add(localClusterName);

        boolean eventPublished = publishEventInternal(eventObject);
        if (eventPublished) {
            wanCounter.incrementCounters(backupEvent);
        }
    }

    /**
     * Publishes the {@code eventObject} onto the WAN replication queues. WAN
     * sync events should not be published using this method.
     *
     * @param eventObject the WAN event
     * @return {@code true} if the event has been published, otherwise returns {@code false}
     */
    private boolean publishEventInternal(InternalWanEvent eventObject) {
        assert !(eventObject instanceof WanEnterpriseMapMerkleTreeNode)
                : "Merkle tree sync objects should not be published";
        assert !(eventObject instanceof WanEnterpriseMapSyncEvent)
                : "Sync objects should not be published";

        if (eventObject instanceof WanEnterpriseMapEvent) {
            String mapName = ((WanEnterpriseMapEvent) eventObject).getMapName();
            int partitionId = getPartitionId(eventObject.getKey());
            return eventQueueContainer.publishMapWanEvent(mapName, partitionId, eventObject);
        }

        if (eventObject instanceof WanCacheEvent) {
            String cacheName = ((WanCacheEvent) eventObject).getNameWithPrefix();
            int partitionId = getPartitionId(eventObject.getKey());
            return eventQueueContainer.publishCacheWanEvent(cacheName, partitionId, eventObject);
        }

        logger.warning("Unexpected replication event object type: " + eventObject.getClass().getName());

        return false;
    }

    /**
     * Increments the counter for a single map or cache and event type.
     * The map/cache and event type is determined by the given {@code event}.
     *
     * @param event the WAN event
     */
    void incrementEventCount(InternalWanEvent event) {
        String serviceName = event.getServiceName();
        WanEventCounters counters
                = wanService.getSentEventCounters(wanReplicationName, wanPublisherId, serviceName);
        event.incrementEventCount(counters);
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
                && queueFullBehavior != WanQueueFullBehavior.THROW_EXCEPTION) {
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
    // public for testing purposes
    protected void finalizeWanEventReplication(Collection<InternalWanEvent>... eventCollections) {
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
        List<InternalCompletableFuture> futures = new ArrayList<>(eventCounts.size());

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
     * Updates the statistics for the provided replicated WAN events and counts
     * them by partition ID and distributed object.
     *
     * @param eventCollections collections of replicated WAN events
     * @return the number of replicated events, per partition and distributed object
     */
    private Map<Integer, Map<DistributedObjectIdentifier, Integer>> updateStatsAndCountEvents(
            Collection<InternalWanEvent>... eventCollections) {
        Map<Integer, Map<DistributedObjectIdentifier, Integer>> eventCounts
                = new HashMap<>();

        for (Collection<InternalWanEvent> events : eventCollections) {
            for (InternalWanEvent event : events) {
                if (event instanceof WanEnterpriseMapSyncEvent
                        || event instanceof WanEnterpriseMapMerkleTreeNode) {
                    syncSupport.removeReplicationEvent((WanEnterpriseMapEvent) event);
                    continue;
                }
                updateStats(event);

                int partitionId = getPartitionId(event.getKey());
                if (!eventCounts.containsKey(partitionId)) {
                    eventCounts.put(partitionId, new HashMap<>());
                }

                Map<DistributedObjectIdentifier, Integer> partitionEventCounts = eventCounts.get(partitionId);

                DistributedObjectIdentifier id = getDistributedObjectIdentifier(event);

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
    private DistributedObjectIdentifier getDistributedObjectIdentifier(InternalWanEvent eventObject) {
        DistributedObjectIdentifier id = null;
        if (eventObject instanceof WanEnterpriseMapEvent) {
            WanEnterpriseMapEvent mapEvent = (WanEnterpriseMapEvent) eventObject;
            id = new DistributedObjectIdentifier(
                    MapService.SERVICE_NAME, mapEvent.getMapName(), mapEvent.getBackupCount());
        } else if (eventObject instanceof WanCacheEvent) {
            WanCacheEvent cacheEvent = (WanCacheEvent) eventObject;
            id = new DistributedObjectIdentifier(
                    CacheService.SERVICE_NAME, cacheEvent.getNameWithPrefix(), cacheEvent.getBackupCount());
        } else {
            logger.warning("Unexpected replication event object type: " + eventObject.getClass().getName());
        }
        return id;
    }

    /**
     * Update the published count and latency between event creation and publication
     */
    private void updateStats(InternalWanEvent event) {
        long latency = Clock.currentTimeMillis() - event.getCreationTime();
        localWanPublisherStats.incrementPublishedEventCount(latency < 0 ? 0 : latency);
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

    /**
     * {@inheritDoc}
     * Checks the size of the WAN replication queue and throws an exception if
     * it has been reached or crossed.
     *
     * @throws WanQueueFullException if queue capacity has been reached and
     *                                          {@link
     *                                          WanBatchPublisherConfig#getQueueFullBehavior()}
     *                                          is set to {@link WanQueueFullBehavior#THROW_EXCEPTION}
     */
    @Override
    public void doPrepublicationChecks() {
        if (isThrowExceptionBehavior(queueFullBehavior) && wanCounter.getPrimaryElementCount() >= queueCapacity) {
            throw new WanQueueFullException(
                    String.format("WAN replication for WAN publisher %s is full. Queue capacity is %d",
                            wanPublisherId, queueCapacity));
        }
    }

    /**
     * {@inheritDoc}
     * This implementation achieves ordering of keys in the same partition by
     * sending {@link WanPutOperation} to the partition owner. The partition ID
     * is equal to the partition for the key contained in the {@code wanReplicationEvent}.
     *
     * @param wanEvent the WAN event to publish
     */
    @Override
    public void republishReplicationEvent(WanEvent wanEvent) {
        InternalWanEvent replicationEventObject
                = (InternalWanEvent) wanEvent;
        WanPutOperation wanPutOperation =
                new WanPutOperation(wanReplicationName, wanPublisherId,
                        wanEvent,
                        replicationEventObject.getBackupCount());
        invokeOnPartition(wanEvent.getServiceName(), replicationEventObject.getKey(), wanPutOperation);
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

    private boolean isThrowExceptionBehavior(WanQueueFullBehavior queueFullBehavior) {
        return WanQueueFullBehavior.THROW_EXCEPTION == queueFullBehavior
                || (WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE == queueFullBehavior
                && state.isReplicateEnqueuedEvents());
    }


    @Override
    public void destroyMapData(String mapName) {
        syncSupport.destroyMapData(mapName);
    }

    @Override
    public int removeWanEvents() {
        boolean isReplicating = state.isReplicateEnqueuedEvents();
        if (isReplicating) {
            pause();
        }

        int drained = clearQueuesInternal();

        if (isReplicating) {
            resume();
        }
        return drained;
    }

    /**
     * Decrements the primary or backup counter by the given {@code delta}.
     *
     * @param delta     the delta by which we need to decrement the counter
     * @param isPrimary {@code true} if the primary WAN event queue counter needs to be decremented,
     *                  {@code false} if the backup WAN event queue counter needs to be decremented
     */
    private void decrementCounter(int delta, boolean isPrimary) {
        if (isPrimary) {
            wanCounter.decrementPrimaryElementCounter(delta);
        } else {
            wanCounter.decrementBackupElementCounter(delta);
        }
    }

    private int clearQueuesInternal() {
        int totalDrained = 0;

        Map<Integer, Integer> drainedPerPartition = eventQueueContainer.drainQueues();
        for (Map.Entry<Integer, Integer> entry : drainedPerPartition.entrySet()) {
            Integer partitionId = entry.getKey();
            int drainedElements = entry.getValue();
            totalDrained += drainedElements;

            InternalPartition partition = node.getPartitionService().getPartition(partitionId);

            decrementCounter(drainedElements, partition.isLocal());
        }

        logger.info("Cleared " + totalDrained + " elements from the WAN queues. Current element counts:"
                + " primary=" + wanCounter.getPrimaryElementCount()
                + " backup=" + wanCounter.getBackupElementCount());
        return totalDrained;
    }

    @Override
    public void onMigrationStart(PartitionMigrationEvent event) {
        wanQueueMigrationSupport.onMigrationStart(event);
    }

    @Override
    public void onMigrationCommit(PartitionMigrationEvent event) {
        wanQueueMigrationSupport.onMigrationCommit(event);
    }

    @Override
    public void onMigrationRollback(PartitionMigrationEvent event) {
        wanQueueMigrationSupport.onMigrationRollback(event);
    }

    /**
     * {@inheritDoc}
     * Removes all WAN events from the partition queues and resets the WAN counters.
     */
    @Override
    public void reset() {
        eventQueueContainer.clear();
        wanCounter.setPrimaryElementCounter(0);
        wanCounter.setBackupElementCounter(0);
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
        eventQueueContainer.collectAllServiceNamespaces(event, namespaces);
    }

    @Override
    public int removeWanEvents(int partitionId, String serviceName, String objectName, int count) {
        boolean isMapService = MapService.SERVICE_NAME.equals(serviceName);
        if (!isMapService && !CacheService.SERVICE_NAME.equals(serviceName)) {
            String msg = "Unexpected replication event service name: " + serviceName;
            assert false : msg;
            logger.warning(msg);
            return 0;
        }

        int removed = 0;
        for (int i = 0; i < count; i++) {
            InternalWanEvent event = isMapService
                    ? eventQueueContainer.pollMapWanEvent(objectName, partitionId)
                    : eventQueueContainer.pollCacheWanEvent(objectName, partitionId);
            if (event != null) {
                wanCounter.decrementBackupElementCounter();
                removed++;
            } else {
                break;
            }
        }
        return removed;
    }

    @Override
    public WanEventMigrationContainer prepareEventContainerReplicationData(
            PartitionReplicationEvent event,
            Collection<ServiceNamespace> namespaces) {
        return eventQueueContainer.prepareEventContainerReplicationData(event, namespaces);
    }

    @Override
    public void processEventContainerReplicationData(int partitionId, WanEventMigrationContainer eventContainer) {
        boolean isPrimaryReplica = node.getNodeEngine().getPartitionService()
                                       .getPartition(partitionId)
                                       .isLocal();
        PartitionWanEventQueueMap mapQueues = eventContainer.getMapQueues();
        PartitionWanEventQueueMap cacheQueues = eventContainer.getCacheQueues();
        int removedMapEvents = isNullOrEmpty(mapQueues)
                ? 0
                : removeWanEvents(partitionId, MapService.SERVICE_NAME);
        int removedCacheEvents = isNullOrEmpty(cacheQueues)
                ? 0
                : removeWanEvents(partitionId, ICacheService.SERVICE_NAME);
        decrementCounter(removedMapEvents + removedCacheEvents, isPrimaryReplica);
        publishEvents(partitionId, mapQueues);
        publishEvents(partitionId, cacheQueues);
    }

    /**
     * Removes all WAN events awaiting replication and belonging to the provided
     * service and partition.
     * If the publisher does not store WAN events, this method is a no-op.
     * Invoked when migrating WAN replication data between members in a cluster.
     *
     * @param serviceName the service name of the WAN events should be removed
     * @param partitionId the partition ID of the WAN events should be removed
     */
    private int removeWanEvents(int partitionId, String serviceName) {
        int size = 0;
        switch (serviceName) {
            case MapService.SERVICE_NAME:
                size += eventQueueContainer.drainMapQueuesMatchingPredicate(partitionId, q -> true);
                break;
            case ICacheService.SERVICE_NAME:
                size += eventQueueContainer.drainCacheQueuesMatchingPredicate(partitionId, q -> true);
                break;
            default:
                String msg = "Unexpected replication event service name: " + serviceName;
                assert false : msg;
                logger.warning(msg);
                break;
        }

        return size;
    }

    private void publishEvents(int partitionId, PartitionWanEventQueueMap queues) {
        if (queues != null) {
            for (WanEventQueue queue : queues.values()) {
                publishReplicationEventQueue(partitionId, queue);
            }
        }
    }


    private void publishReplicationEventQueue(int partitionId, WanEventQueue eventQueue) {
        InternalWanEvent event = eventQueue.poll();
        while (event != null) {
            boolean isPrimaryReplica = node.getNodeEngine().getPartitionService()
                                           .getPartition(partitionId)
                                           .isLocal();
            // whether the event is published as a backup or primary only
            // affects if the event is counted as a backup or primary event
            // we check the local (transient) partition table and publish accordingly
            // the WanQueueMigrationSupport.onMigrationCommit will then migrate
            // some of the counts from backup to primary counter and vice versa
            if (isPrimaryReplica) {
                publishReplicationEvent(event);
            } else {
                publishReplicationEventBackup(event);
            }
            event = eventQueue.poll();
        }
    }

    /**
     * Creates the instance implementing WAN sync features. This method must
     * not return {@code null}.
     */
    protected abstract WanPublisherSyncSupport createWanSyncSupport();

    /**
     * Returns the WAN configuration context for the configuration used to
     * instantiate this publisher. Public for testing.
     */
    public WanConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    // public for testing
    public PollSynchronizerPublisherQueueContainer getEventQueueContainer() {
        return eventQueueContainer;
    }
}
