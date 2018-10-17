package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.events.WanFullSyncFinishedEvent;
import com.hazelcast.internal.management.events.WanSyncProgressUpdateEvent;
import com.hazelcast.internal.management.events.WanSyncStartedEvent;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.SetUtil;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Support class for processing WAN sync events for a single publisher.
 */
public class WanPublisherFullSyncSupport implements WanPublisherSyncSupport {

    private final NodeEngineImpl nodeEngine;
    private final MapService mapService;
    /**
     * The count of {@link WanReplicationEvent} sync events pending replication per partition.
     */
    private final Map<Integer, AtomicInteger> counterMap = new ConcurrentHashMap<Integer, AtomicInteger>();

    private final WanSyncManager syncManager;
    private final AbstractWanPublisher publisher;
    private final Map<String, WanSyncStats> lastSyncStats = new ConcurrentHashMap<String, WanSyncStats>();

    WanPublisherFullSyncSupport(Node node, AbstractWanPublisher publisher) {
        this.nodeEngine = node.getNodeEngine();
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.publisher = publisher;
        final EnterpriseWanReplicationService service =
                (EnterpriseWanReplicationService) nodeEngine.getWanReplicationService();
        this.syncManager = service.getSyncManager();
    }

    @Override
    public void destroyMapData(String mapName) {
        // NOOP
    }

    /**
     * Processes the WAN sync event and updates the {@code result} with the
     * processing results.
     *
     * @param event  WAN sync event
     * @param result the processing result
     */
    public void processEvent(WanSyncEvent event, WanAntiEntropyEventResult result) {
        final Collection<String> mapNames = getMapsToSynchronize(event);
        Map<String, FullWanSyncStats> mapSyncStats = MapUtil.createHashMap(mapNames.size());

        beforeSync(mapNames, mapSyncStats);

        syncManager.resetSyncedPartitionCount();
        Set<Integer> syncedPartitions = result.getProcessedPartitions();
        Set<Integer> partitionsToSync = event.getPartitionSet();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();

        IPartition[] partitions = getPartitions(partitionService, partitionsToSync);
        int countLocalPartitions = getLocalPartitionCount(partitions);
        for (IPartition partition : partitions) {
            syncPartition(event, syncedPartitions, partition, countLocalPartitions, mapSyncStats);
        }

        afterSync(mapNames, mapSyncStats);
    }

    private Collection<String> getMapsToSynchronize(WanSyncEvent event) {
        final Collection<String> mapNames;
        if (event.getType() == WanSyncType.ALL_MAPS) {
            mapNames = mapService.getMapServiceContext().getMapContainers().keySet();
        } else {
            mapNames = SetUtil.createHashSet(1);
            mapNames.add(event.getMapName());
        }
        return mapNames;
    }

    private void beforeSync(Collection<String> mapNames, Map<String, FullWanSyncStats> mapSyncStats) {
        for (String mapName : mapNames) {
            nodeEngine.getManagementCenterService()
                      .log(new WanSyncStartedEvent(publisher.wanReplicationName, publisher.wanPublisherId, mapName));
            mapSyncStats.put(mapName, new FullWanSyncStats());
        }
    }

    private void afterSync(Collection<String> mapNames, Map<String, FullWanSyncStats> mapSyncStats) {
        for (String mapName : mapNames) {
            FullWanSyncStats syncStats = mapSyncStats.get(mapName);
            syncStats.onSyncComplete();
            lastSyncStats.put(mapName, syncStats);
            writeMcSyncFinishedEvent(mapName, syncStats);
        }
    }

    private void writeMcSyncFinishedEvent(String mapName, FullWanSyncStats syncStats) {
        WanFullSyncFinishedEvent syncFinishedEvent = new WanFullSyncFinishedEvent(publisher.wanReplicationName,
                publisher.wanPublisherId, mapName, syncStats.getDurationSecs(), syncStats.getRecordsSynced(),
                syncStats.getPartitionsSynced());
        nodeEngine.getManagementCenterService()
                  .log(syncFinishedEvent);
    }

    private IPartition[] getPartitions(InternalPartitionService partitionService, Set<Integer> partitionsToSync) {
        if (CollectionUtil.isEmpty(partitionsToSync)) {
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
    public void processEvent(WanConsistencyCheckEvent event, WanAntiEntropyEventResult result) {
        // NOOP
    }

    @Override
    public Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults() {
        return null;
    }

    @Override
    public Map<String, WanSyncStats> getLastSyncStats() {
        return lastSyncStats;
    }

    @Override
    public void removeReplicationEvent(EnterpriseMapReplicationObject sync) {
        int partitionId = ((EnterpriseMapReplicationSync) sync).getPartitionId();
        int remainingEventCount = counterMap.get(partitionId).decrementAndGet();
        if (remainingEventCount == 0) {
            syncManager.incrementSyncedPartitionCount();
        }
    }

    /**
     * Enqueues WAN sync events for the given keys and partition for replication
     * if this member is the owner of the partition.
     * Once events have been enqueued the partition ID is added to the
     * {@code syncedPartitions}.
     * @param event            the WAN sync event
     * @param syncedPartitions the set of synced partition IDs
     * @param partition        the partition to sync
     * @param countLocalPartitions the number of the local partitions
     * @param mapSyncStats the sync statistics per synced maps to update
     */
    private void syncPartition(WanSyncEvent event,
                               Set<Integer> syncedPartitions,
                               IPartition partition,
                               int countLocalPartitions,
                               Map<String, FullWanSyncStats> mapSyncStats) {
        if (partition.isLocal()) {
            syncPartition(event, partition, mapSyncStats, countLocalPartitions);
            syncedPartitions.add(partition.getPartitionId());
        }
    }

    /**
     * Syncs the {@code partition} for all maps or a specific map, depending
     * on {@link WanSyncEvent#getType()}
     */
    private void syncPartition(WanSyncEvent syncEvent,
                               IPartition partition,
                               Map<String, FullWanSyncStats> mapSyncStats, int countLocalPartitions) {
        int partitionEventCount = 0;
        counterMap.put(partition.getPartitionId(), new AtomicInteger());
        if (syncEvent.getType() == WanSyncType.ALL_MAPS) {
            for (Map.Entry<String, FullWanSyncStats> entry : mapSyncStats.entrySet()) {
                String mapName = entry.getKey();
                FullWanSyncStats syncStats = entry.getValue();
                partitionEventCount += syncPartitionForMap(mapName, partition, syncStats, countLocalPartitions);
            }
        } else {
            String mapName = syncEvent.getMapName();
            FullWanSyncStats syncStats = mapSyncStats.get(mapName);
            partitionEventCount += syncPartitionForMap(mapName, partition, syncStats, countLocalPartitions);
        }
        if (partitionEventCount == 0) {
            syncManager.incrementSyncedPartitionCount();
        }
    }

    /**
     * Gets all map partition data and offers it to the staging queue, blocking
     * until all entries have been offered.
     *
     * @param mapName   the map name
     * @param partition the partition for which entries should be enqueued
     * @param syncStats the synchronization statistics to update
     * @param countLocalPartitions the number of the local partitions
     * @return the number of enqueued sync events
     */
    private int syncPartitionForMap(String mapName, IPartition partition, FullWanSyncStats syncStats, int countLocalPartitions) {
        GetMapPartitionDataOperation op = new GetMapPartitionDataOperation(mapName);
        int partitionId = partition.getPartitionId();
        op.setPartitionId(partitionId);
        Set<SimpleEntryView<Data, Data>> set = invokeOp(op);
        int syncedEntries = set.size();
        counterMap.get(partitionId).addAndGet(syncedEntries);

        for (SimpleEntryView<Data, Data> simpleEntryView : set) {
            EnterpriseMapReplicationSync sync = new EnterpriseMapReplicationSync(mapName, simpleEntryView, partitionId);
            WanReplicationEvent event = new WanReplicationEvent(MapService.SERVICE_NAME, sync);
            publisher.offerToStagingQueue(event);
        }

        syncStats.onSyncPartition();
        syncStats.onSyncRecords(syncedEntries);
        nodeEngine.getManagementCenterService()
                  .log(new WanSyncProgressUpdateEvent(publisher.wanReplicationName, publisher.wanPublisherId,
                          mapName, countLocalPartitions, syncStats.getPartitionsSynced()));

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
