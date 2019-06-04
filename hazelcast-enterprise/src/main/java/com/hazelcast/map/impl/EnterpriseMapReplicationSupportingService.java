package com.hazelcast.map.impl;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * This class handles incoming map WAN replication events.
 */
public class EnterpriseMapReplicationSupportingService implements ReplicationSupportingService {
    private static final String DEFAULT_MERGE_POLICY = PassThroughMergePolicy.class.getName();

    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;
    private final MapServiceContext mapServiceContext;
    private final DistributedServiceWanEventCounters wanEventCounters;
    private final SplitBrainMergePolicy<Data, MapMergeTypes> defaultSyncMergePolicy;

    EnterpriseMapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        MergePolicyProvider mergePolicyProvider = mapServiceContext.getMergePolicyProvider();
        //noinspection unchecked
        this.defaultSyncMergePolicy
                = (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicyProvider
                .getMergePolicy(DEFAULT_MERGE_POLICY);
        this.proxyService = nodeEngine.getProxyService();
        this.wanEventCounters = nodeEngine.getWanReplicationService()
                                          .getReceivedEventCounters(MapService.SERVICE_NAME);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (!(eventObject instanceof EnterpriseMapReplicationObject)) {
            return;
        }
        EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
        String mapName = mapReplicationObject.getMapName();

        /*
          Proxies should be created to initialize listeners, indexes, etc. and to show WAN replicated maps in mancenter.
          Otherwise, users are forced to manually call IMap#get()
          Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
         */
        proxyService.getDistributedObject(MapService.SERVICE_NAME, mapName);

        if (eventObject instanceof EnterpriseMapReplicationSync) {
            handleSyncEvent((EnterpriseMapReplicationSync) eventObject);
            wanEventCounters.incrementSync(mapName);
            return;
        }

        if (eventObject instanceof EnterpriseMapReplicationMerkleTreeNode) {
            EnterpriseMapReplicationMerkleTreeNode merkleTreeNode
                    = (EnterpriseMapReplicationMerkleTreeNode) eventObject;
            handleMerkleTreeNode(merkleTreeNode);
            wanEventCounters.incrementSync(mapName, merkleTreeNode.getEntryCount());
            return;
        }

        republishIfNecessary(replicationEvent, mapName);

        if (eventObject instanceof EnterpriseMapReplicationUpdate) {
            handleUpdateEvent((EnterpriseMapReplicationUpdate) eventObject, replicationEvent.getAcknowledgeType());
            wanEventCounters.incrementUpdate(mapName);
        } else if (eventObject instanceof EnterpriseMapReplicationRemove) {
            handleRemoveEvent((EnterpriseMapReplicationRemove) eventObject, replicationEvent.getAcknowledgeType());
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
    private void handleMerkleTreeNode(EnterpriseMapReplicationMerkleTreeNode event) {
        String mapName = event.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        for (EntryView<Data, Data> entryView : event.getEntries().getNodeEntries()) {
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
    private void republishIfNecessary(WanReplicationEvent event, String mapName) {
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (mapContainer.isWanRepublishingEnabled()) {
            mapContainer.getWanReplicationPublisher().publishReplicationEvent(event);
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
    private void handleUpdateEvent(EnterpriseMapReplicationUpdate event, WanAcknowledgeType acknowledgeType) {
        final String mapName = event.getMapName();
        final MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        final Object mergePolicy = event.getMergePolicy();
        final Data key;
        final Operation operation;

        if (mergePolicy instanceof SplitBrainMergePolicy) {
            MapMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
            key = mergingEntry.getKey();
            //noinspection unchecked
            operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                    (SplitBrainMergePolicy<Data, MapMergeTypes>) mergePolicy, true);
        } else {
            EntryView<Data, Data> entryView = event.getEntryView();
            key = entryView.getKey();
            operation = operationProvider.createLegacyMergeOperation(mapName, entryView,
                    (MapMergePolicy) mergePolicy, true);
        }

        final InternalCompletableFuture future = invokeOnPartition(key, operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.join();
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
    private void handleRemoveEvent(EnterpriseMapReplicationRemove event, WanAcknowledgeType acknowledgeType) {
        final String mapName = event.getMapName();
        final MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        final MapOperation operation = operationProvider.createDeleteOperation(mapName, event.getKey(), true);
        final InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.join();
        }
    }

    /**
     * Processes a WAN sync event by forwarding it to the partition owner.
     * The method will return as soon as the event has been forwarded to the
     * partition owner and it does not wait for the event to be processed.
     *
     * @param event the WAN sync event
     */
    private void handleSyncEvent(EnterpriseMapReplicationSync event) {
        String mapName = event.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
        MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry, defaultSyncMergePolicy, true);
        invokeOnPartition(mergingEntry.getKey(), operation);
    }

    /**
     * Invokes the {@code operation} on the partition owner for the partition
     * owning the {@code key}.
     *
     * @param key       the key on which partition the operation is invoked
     * @param operation the operation to invoke
     * @return the future representing the pending completion of the operation
     */
    private InternalCompletableFuture invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService()
                             .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
