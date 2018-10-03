package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
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
    public void processEvent(WanSyncEvent event,
                             WanAntiEntropyEventResult result) {
        syncManager.resetSyncedPartitionCount();
        Set<Integer> syncedPartitions = result.getProcessedPartitions();
        Set<Integer> partitionsToSync = event.getPartitionSet();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        FullWanSyncStats syncStats = new FullWanSyncStats();

        if (CollectionUtil.isEmpty(partitionsToSync)) {
            for (IPartition partition : partitionService.getPartitions()) {
                syncPartition(event, syncedPartitions, partition, syncStats);
            }
        } else {
            for (Integer partitionId : partitionsToSync) {
                IPartition partition = partitionService.getPartition(partitionId);
                syncPartition(event, syncedPartitions, partition, syncStats);
            }
        }

        syncStats.onSyncComplete();
        lastSyncStats.put(event.getMapName(), syncStats);
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
     *
     * @param event            the WAN sync event
     * @param syncedPartitions the set of synced partition IDs
     * @param partition        the partition to sync
     */
    private void syncPartition(WanSyncEvent event,
                               Set<Integer> syncedPartitions,
                               IPartition partition,
                               FullWanSyncStats syncStats) {
        if (partition.isLocal()) {
            syncPartition(event, partition, syncStats);
            syncedPartitions.add(partition.getPartitionId());
            syncStats.onSyncPartition();
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
