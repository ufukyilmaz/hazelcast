package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.ManagementCenterService;
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
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;

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
    private final ManagementCenterService managementCenterService;

    WanPublisherFullSyncSupport(Node node, AbstractWanPublisher publisher) {
        this.nodeEngine = node.getNodeEngine();
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.publisher = publisher;
        final EnterpriseWanReplicationService service =
                (EnterpriseWanReplicationService) nodeEngine.getWanReplicationService();
        this.syncManager = service.getSyncManager();
        this.managementCenterService = nodeEngine.getManagementCenterService();
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
    public void processEvent(WanSyncEvent event,
                             WanAntiEntropyEventResult result) {
        managementCenterService.log(new WanSyncStartedEvent(publisher.wanReplicationName,
                publisher.wanPublisherId, event.getMapName()));

        syncManager.resetSyncedPartitionCount();
        Set<Integer> syncedPartitions = result.getProcessedPartitions();
        Set<Integer> partitionsToSync = event.getPartitionSet();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        FullWanSyncStats syncStats = new FullWanSyncStats();

        IPartition[] partitions = getPartitions(partitionService, partitionsToSync);
        int countLocalPartitions = getLocalPartitionCount(partitions);
        for (IPartition partition : partitions) {
            syncPartition(event, syncedPartitions, partition, countLocalPartitions, syncStats);
        }

        syncStats.onSyncComplete();
        lastSyncStats.put(event.getMapName(), syncStats);
        writeMcSyncFinishedEvent(event, syncStats);
    }

    private void writeMcSyncFinishedEvent(WanSyncEvent event, FullWanSyncStats syncStats) {
        WanFullSyncFinishedEvent syncFinishedEvent = new WanFullSyncFinishedEvent(publisher.wanReplicationName,
                publisher.wanPublisherId, event.getMapName(), syncStats.getDurationSecs(), syncStats.getRecordsSynced(),
                syncStats.getPartitionsSynced());
        managementCenterService.log(syncFinishedEvent);
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
     */
    private void syncPartition(WanSyncEvent event,
                               Set<Integer> syncedPartitions,
                               IPartition partition,
                               int countLocalPartitions,
                               FullWanSyncStats syncStats) {
        if (partition.isLocal()) {
            syncPartition(event, partition, syncStats);
            syncedPartitions.add(partition.getPartitionId());
            syncStats.onSyncPartition();
            managementCenterService.log(new WanSyncProgressUpdateEvent(publisher.wanReplicationName, publisher
                    .wanPublisherId, event.getMapName(), countLocalPartitions, syncStats.getPartitionsSynced()));
        }
    }

    /**
     * Syncs the {@code partition} for all maps or a specific map, depending
     * on {@link WanSyncEvent#getType()}
     */
    private void syncPartition(WanSyncEvent syncEvent,
                               IPartition partition,
                               FullWanSyncStats syncStats) {
        int partitionEventCount = 0;
        counterMap.put(partition.getPartitionId(), new AtomicInteger());
        if (syncEvent.getType() == WanSyncType.ALL_MAPS) {
            for (String mapName : mapService.getMapServiceContext().getMapContainers().keySet()) {
                partitionEventCount += syncPartitionForMap(mapName, partition);
            }
        } else {
            partitionEventCount += syncPartitionForMap(syncEvent.getMapName(), partition);
        }
        if (partitionEventCount == 0) {
            syncManager.incrementSyncedPartitionCount();
        }
        syncStats.onSyncRecords(partitionEventCount);
    }

    /**
     * Gets all map partition data and offers it to the staging queue, blocking
     * until all entries have been offered.
     *
     * @param mapName   the map name
     * @param partition the partition for which entries should be enqueued
     * @return the number of enqueued sync events
     */
    private int syncPartitionForMap(String mapName,
                                    IPartition partition) {
        GetMapPartitionDataOperation op = new GetMapPartitionDataOperation(mapName);
        op.setPartitionId(partition.getPartitionId());
        Set<SimpleEntryView<Data, Data>> set = invokeOp(op);
        counterMap.get(partition.getPartitionId()).addAndGet(set.size());

        for (SimpleEntryView<Data, Data> simpleEntryView : set) {
            EnterpriseMapReplicationSync sync = new EnterpriseMapReplicationSync(
                    mapName, simpleEntryView, partition.getPartitionId());
            WanReplicationEvent event = new WanReplicationEvent(MapService.SERVICE_NAME, sync);
            publisher.offerToStagingQueue(event);
        }
        return set.size();
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
