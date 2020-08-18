package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventResult;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.events.WanFullSyncFinishedEvent;
import com.hazelcast.internal.management.events.WanSyncProgressUpdateEvent;
import com.hazelcast.internal.management.events.WanSyncStartedEvent;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.wan.WanEnterpriseMapEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapSyncEvent;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import com.hazelcast.wan.impl.WanSyncStats;
import com.hazelcast.wan.impl.WanSyncType;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Support class for processing WAN sync events for a single publisher.
 */
public class WanPublisherFullSyncSupport implements WanPublisherSyncSupport {
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final MapService mapService;
    private final WanBatchPublisher publisher;
    private final Map<String, FullWanSyncStats> lastSyncStats = new ConcurrentHashMap<>();
    private final Map<UUID, WanSyncContext<FullWanSyncStats>> syncContextMap = new ConcurrentHashMap<>();
    private final ExecutorService updateSerializingExecutor;

    WanPublisherFullSyncSupport(Node node, WanBatchPublisher publisher) {
        this.nodeEngine = node.getNodeEngine();
        this.updateSerializingExecutor = newSingleThreadExecutor(
                r -> new Thread(r, ThreadUtil.createThreadName(node.hazelcastInstance.getName(), "wan-sync-stats-updater")));
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.publisher = publisher;
        this.logger = nodeEngine.getLogger(WanPublisherFullSyncSupport.class);
    }

    @Override
    public void destroyMapData(String mapName) {
        lastSyncStats.remove(mapName);
    }

    /**
     * {@inheritDoc}
     * Processes the WAN sync event and updates the {@code result} with the
     * processing results.
     *
     * @param event WAN sync event
     */
    @Override
    public void processEvent(WanSyncEvent event) {
        final Collection<String> mapNames = getMapsToSynchronize(event);
        WanAntiEntropyEventResult result = event.getProcessingResult();

        if (!isEmpty(mapNames)) {
            Set<Integer> syncedPartitions = result.getProcessedPartitions();
            Set<Integer> partitionsToSync = event.getPartitionSet();
            InternalPartitionService partitionService = nodeEngine.getPartitionService();

            IPartition[] partitions = getPartitions(partitionService, partitionsToSync);
            int countLocalPartitions = getLocalPartitionCount(partitions);

            beforeSync(event.getUuid(), mapNames, countLocalPartitions);

            for (IPartition partition : partitions) {
                syncPartition(event, syncedPartitions, partition);
            }
        }
    }

    private Collection<String> getMapsToSynchronize(WanSyncEvent event) {
        final Collection<String> mapNames;
        if (event.getType() == WanSyncType.ALL_MAPS) {
            Collection<String> allMapNames = mapService.getMapServiceContext().getMapContainers().keySet();
            mapNames = SetUtil.createHashSet(allMapNames.size());
            for (String mapName : allMapNames) {
                if (isMapWanReplicated(mapName)) {
                    mapNames.add(mapName);
                }
            }
        } else {
            String mapName = event.getObjectName();
            if (!isMapWanReplicated(mapName)) {
                throw new IllegalArgumentException("WAN synchronization requested for map " + mapName + " that is "
                        + "not configured for WAN replication");
            }
            mapNames = SetUtil.createHashSet(1);
            mapNames.add(mapName);
        }
        return mapNames;
    }

    private boolean isMapWanReplicated(String mapName) {
        return mapService.getMapServiceContext().getMapContainer(mapName).isWanReplicationEnabled();
    }

    private void beforeSync(UUID uuid, Collection<String> mapNames, int countLocalPartitions) {
        WanSyncContext<FullWanSyncStats> syncContext = new WanSyncContext<>(uuid, countLocalPartitions, mapNames);
        syncContextMap.put(uuid, syncContext);

        for (String mapName : mapNames) {
            nodeEngine.getManagementCenterService()
                      .log(new WanSyncStartedEvent(uuid, publisher.wanReplicationName, publisher.wanPublisherId, mapName));
            FullWanSyncStats syncStats = new FullWanSyncStats(uuid, countLocalPartitions);
            syncContext.addSyncStats(mapName, syncStats);
            lastSyncStats.put(mapName, syncStats);
        }
    }

    private void writeMcSyncFinishedEvent(UUID uuid, String mapName, FullWanSyncStats syncStats) {
        WanFullSyncFinishedEvent syncFinishedEvent = new WanFullSyncFinishedEvent(uuid, publisher.wanReplicationName,
                publisher.wanPublisherId, mapName, syncStats.getDurationSecs(), syncStats.getRecordsSynced(),
                syncStats.getPartitionsSynced());
        nodeEngine.getManagementCenterService()
                  .log(syncFinishedEvent);
    }

    private IPartition[] getPartitions(InternalPartitionService partitionService, Set<Integer> partitionsToSync) {
        if (isEmpty(partitionsToSync)) {
            return partitionService.getPartitions();
        }

        IPartition[] partitions = new IPartition[partitionsToSync.size()];
        int partitionIdx = 0;
        for (int partitionId : partitionsToSync) {
            partitions[partitionIdx++] = partitionService.getPartition(partitionId);
        }

        return partitions;
    }

    private int getLocalPartitionCount(IPartition[] partitions) {
        int count = 0;
        for (IPartition partition : partitions) {
            if (partition.isLocal()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void processEvent(WanConsistencyCheckEvent event) {
        // NOOP
    }

    @Override
    public Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults() {
        return null;
    }

    @Override
    public Map<String, WanSyncStats> getLastSyncStats() {
        return unmodifiableMap(lastSyncStats);
    }

    @Override
    public void removeReplicationEvent(WanEnterpriseMapEvent replicationObject) {
        WanEnterpriseMapSyncEvent sync = (WanEnterpriseMapSyncEvent) replicationObject;
        WanSyncContext<FullWanSyncStats> syncContext = syncContextMap.get(sync.getUuid());
        String mapName = sync.getMapName();
        int partitionId = sync.getPartitionId();
        int remainingEventCount = syncContext.getSyncCounter(mapName, partitionId).decrementAndGet();
        FullWanSyncStats syncStats = syncContext.getSyncStats(mapName);
        syncStats.onSyncRecord();

        updateSerializingExecutor.execute(() -> {
            if (remainingEventCount == 0) {
                int partitionsSynced = syncStats.onSyncPartition();

                WanSyncProgressUpdateEvent updateEvent = new WanSyncProgressUpdateEvent(syncContext.getUuid(),
                        publisher.wanReplicationName, publisher.wanPublisherId, mapName, syncStats.getPartitionsToSync(),
                        partitionsSynced, syncStats.getRecordsSynced());
                nodeEngine.getManagementCenterService().log(updateEvent);

                completeSyncContext(syncContext, mapName, syncStats, partitionsSynced);
            }
        });
    }

    private void completeSyncContext(WanSyncContext<FullWanSyncStats> syncContext, String mapName, FullWanSyncStats syncStats,
                                     int partitionsSynced) {
        if (syncStats.getPartitionsToSync() == partitionsSynced) {
            syncContext.onMapSynced();
            syncStats.onSyncComplete();
            logSyncStats(syncStats);
            writeMcSyncFinishedEvent(syncContext.getUuid(), mapName, syncStats);
            cleanupSyncContextMap();
        }
    }

    private void cleanupSyncContextMap() {
        for (Map.Entry<UUID, WanSyncContext<FullWanSyncStats>> entry : syncContextMap.entrySet()) {
            UUID key = entry.getKey();
            WanSyncContext<FullWanSyncStats> context = entry.getValue();
            if (context.isCompletedOrStuck()) {
                syncContextMap.remove(key);
            }
        }
    }

    private void logSyncStats(FullWanSyncStats stats) {
        String syncStatsMsg = String.format("Synchronization finished%n%n"
                        + "Synchronization statistics:%n"
                        + "\t Synchronization UUID: %s%n"
                        + "\t Duration: %d secs%n"
                        + "\t Total records synchronized: %d%n"
                        + "\t Total partitions synchronized: %d%n",
                stats.getUuid(), stats.getDurationSecs(), stats.getRecordsSynced(), stats.getPartitionsSynced());
        logger.info(syncStatsMsg);
    }

    /**
     * Enqueues WAN sync events for the given keys and partition for replication
     * if this member is the owner of the partition.
     * Once events have been enqueued the partition ID is added to the
     * {@code syncedPartitions}.
     * @param event            the WAN sync event
     * @param syncedPartitions the set of synced partition IDs
     * @param partition        the partition to sync
     */
    private void syncPartition(WanSyncEvent event, Set<Integer> syncedPartitions, IPartition partition) {
        if (partition.isLocal()) {
            syncPartition(event, partition);
            syncedPartitions.add(partition.getPartitionId());
        }
    }

    /**
     * Syncs the {@code partition} for all maps or a specific map, depending
     * on {@link WanSyncEvent#getType()}
     */
    private void syncPartition(WanSyncEvent syncEvent, IPartition partition) {
        UUID eventUuid = syncEvent.getUuid();
        WanSyncContext<FullWanSyncStats> syncContext = syncContextMap.get(eventUuid);
        if (syncEvent.getType() == WanSyncType.ALL_MAPS) {
            for (String mapName : syncContext.getMapNames()) {
                syncPartitionForMap(syncContext, mapName, partition);
            }
        } else {
            String mapName = syncEvent.getObjectName();
            syncPartitionForMap(syncContext, mapName, partition);
        }
    }

    /**
     * Gets all map partition data and offers it to the staging queue, blocking
     * until all entries have been offered.
     *
     *
     * @param syncContext the synchronization context of the given synchronization process
     * @param mapName     the map name
     * @param partition   the partition for which entries should be enqueued
     * @return the number of enqueued sync events
     */
    private int syncPartitionForMap(WanSyncContext<FullWanSyncStats> syncContext, String mapName, IPartition partition) {
        GetMapPartitionDataOperation op = new GetMapPartitionDataOperation(mapName);
        int partitionId = partition.getPartitionId();
        op.setPartitionId(partitionId);
        Set<WanMapEntryView<Object, Object>> set = invokeOp(op);
        int syncedEntries = set.size();
        syncContext.getSyncCounter(mapName, partitionId).addAndGet(syncedEntries);

        for (WanMapEntryView<Object, Object> entryView : set) {
            WanEnterpriseMapSyncEvent sync = new WanEnterpriseMapSyncEvent(syncContext.getUuid(), mapName, entryView,
                    partitionId);
            publisher.putToSyncEventQueue(sync);
        }

        if (syncedEntries == 0) {
            FullWanSyncStats syncStats = syncContext.getSyncStats(mapName);
            updateSerializingExecutor.execute(() -> {
                int partitionsSynced = syncStats.onSyncPartition();
                WanSyncProgressUpdateEvent updateEvent = new WanSyncProgressUpdateEvent(syncContext.getUuid(),
                        publisher.wanReplicationName, publisher.wanPublisherId, mapName, syncStats.getPartitionsToSync(),
                        partitionsSynced, syncStats.getRecordsSynced());
                nodeEngine.getManagementCenterService().log(updateEvent);
                completeSyncContext(syncContext, mapName, syncStats, partitionsSynced);
            });
        }

        return syncedEntries;
    }

    /**
     * Invokes a partition operation and blocks waiting for the result.
     * The operation service is set to {@value MapService#SERVICE_NAME}.
     *
     * @param operation the operation to invoke
     * @return the result of the invocation
     */
    private <T> T invokeOp(Operation operation) {
        try {
            Future<T> future = nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, operation.getPartitionId())
                    .setResultDeserialized(false)
                    .invoke();
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
