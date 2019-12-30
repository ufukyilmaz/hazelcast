package com.hazelcast.map.impl.wan;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.util.UUID;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * This class handles incoming map WAN replication events.
 */
public class WanEnterpriseMapSupportingService implements WanSupportingService {
    private static final String DEFAULT_MERGE_POLICY = PassThroughMergePolicy.class.getName();

    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;
    private final MapServiceContext mapServiceContext;
    private final WanEventCounters wanEventCounters;
    private final SplitBrainMergePolicy<Data, MapMergeTypes> defaultSyncMergePolicy;
    private final ILogger logger;

    public WanEnterpriseMapSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(this.getClass());
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        this.defaultSyncMergePolicy = (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicyProvider
                .getMergePolicy(DEFAULT_MERGE_POLICY);
        this.proxyService = nodeEngine.getProxyService();
        this.wanEventCounters = nodeEngine.getWanReplicationService()
                .getReceivedEventCounters(MapService.SERVICE_NAME);
    }

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        if (!(event instanceof WanEnterpriseMapEvent)) {
            return;
        }
        WanEnterpriseMapEvent mapReplicationObject = (WanEnterpriseMapEvent) event;
        String mapName = mapReplicationObject.getMapName();

        /*
          Proxies should be created to initialize listeners, indexes, etc. and to show WAN replicated maps in MC.
          Otherwise, users are forced to manually call IMap#get()
          Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
         */
        UUID source = nodeEngine.getLocalMember().getUuid();
        proxyService.getDistributedObject(MapService.SERVICE_NAME, mapName, source);

        if (event instanceof WanEnterpriseMapSyncEvent) {
            handleSyncEvent((WanEnterpriseMapSyncEvent) event);
            wanEventCounters.incrementSync(mapName);
            return;
        }

        if (event instanceof WanEnterpriseMapMerkleTreeNode) {
            WanEnterpriseMapMerkleTreeNode merkleTreeNode
                    = (WanEnterpriseMapMerkleTreeNode) event;
            handleMerkleTreeNode(merkleTreeNode);
            wanEventCounters.incrementSync(mapName, merkleTreeNode.getEntryCount());
            return;
        }

        republishIfNecessary(event, mapName);

        if (event instanceof WanEnterpriseMapAddOrUpdateEvent) {
            handleUpdateEvent((WanEnterpriseMapAddOrUpdateEvent) event, acknowledgeType);
            wanEventCounters.incrementUpdate(mapName);
        } else if (event instanceof WanEnterpriseMapRemoveEvent) {
            handleRemoveEvent((WanEnterpriseMapRemoveEvent) event, acknowledgeType);
            wanEventCounters.incrementRemove(mapName);
        }
    }

    /**
     * Processes a merkle tree node event by forwarding it to the partition owner.
     * The method will return as soon as the event has been forwarded to the
     * partition owner and it does not wait for the event to be processed.
     *
     * @param event the merkle tree node event
     */
    private void handleMerkleTreeNode(WanEnterpriseMapMerkleTreeNode event) {
        String mapName = event.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        for (WanMapEntryView<Object, Object> entryView : event.getEntries().getNodeEntries()) {
            MapMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), entryView);
            MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry, defaultSyncMergePolicy, true);
            invokeOnPartition(mergingEntry.getKey(), operation);
        }
    }

    /**
     * Republishes the WAN {@code event} if configured to do so.
     *
     * @param event   the WAN replication event
     * @param mapName the event map name
     */
    private void republishIfNecessary(InternalWanEvent event, String mapName) {
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (mapContainer.isWanRepublishingEnabled()) {
            mapContainer.getWanReplicationDelegate().republishReplicationEvent(event);
        }
    }

    /**
     * Processes a WAN update event by forwarding it to the partition owner.
     * Depending on the {@code acknowledgeType}, it will either return as soon
     * as the event has been forwarded to the partition owner or block until
     * it has been processed on the partition owner.
     *
     * @param event           the WAN update event
     * @param acknowledgeType determines whether the method will wait for the
     *                        update to be processed on the partition owner
     */
    private void handleUpdateEvent(WanEnterpriseMapAddOrUpdateEvent event, WanAcknowledgeType acknowledgeType) {
        final String mapName = event.getMapName();
        final MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        final Object mergePolicy = event.getMergePolicy();
        final Data key;
        final Operation operation;

        MapMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
        key = mergingEntry.getKey();
        operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicy, true);

        final InternalCompletableFuture future = invokeOnPartition(key, operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.joinInternal();
        }
    }

    /**
     * Processes a WAN remove event by forwarding it to the partition owner.
     * Depending on the {@code acknowledgeType}, it will either return as soon
     * as the event has been forwarded to the partition owner or block until
     * it has been processed on the partition owner.
     *
     * @param event           the WAN remove event
     * @param acknowledgeType determines whether the method will wait for the
     *                        update to be processed on the partition owner
     */
    private void handleRemoveEvent(WanEnterpriseMapRemoveEvent event, WanAcknowledgeType acknowledgeType) {
        final String mapName = event.getMapName();
        final MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        final MapOperation operation = operationProvider.createDeleteOperation(mapName, event.getKey(), true);
        final InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.joinInternal();
        }
    }

    /**
     * Processes a WAN sync event by forwarding it to the partition owner.
     * The method will return as soon as the event has been forwarded to the
     * partition owner and it does not wait for the event to be processed.
     *
     * @param event the WAN sync event
     */
    private void handleSyncEvent(WanEnterpriseMapSyncEvent event) {
        String mapName = event.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
        MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry, defaultSyncMergePolicy, true);
        InternalCompletableFuture<Boolean> future = invokeOnPartition(mergingEntry.getKey(), operation);
        future.whenCompleteAsync((v, t) -> {
            if (t != null) {
                logger.warning("Failed to process WAN sync event", t);
            }
        });
    }

    /**
     * Invokes the {@code operation} on the partition owner for the partition
     * owning the {@code key}.
     *
     * @param key       the key on which partition the operation is invoked
     * @param operation the operation to invoke
     * @return the future representing the pending completion of the operation
     */
    private <E> InternalCompletableFuture<E> invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
