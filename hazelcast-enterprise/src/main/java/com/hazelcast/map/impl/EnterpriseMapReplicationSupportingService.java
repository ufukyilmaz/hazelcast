package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.wan.WanReplicationEvent;

import static com.hazelcast.config.WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * This class handles incoming map WAN replication events.
 */

class EnterpriseMapReplicationSupportingService implements ReplicationSupportingService {

    /**
     * Uses map delete instead of map remove when processing remove events. This has the benefit
     * that the old value will not be sent from the partition owner (since this member does not use it)
     * and that we avoid {@link ClassNotFoundException}s when the in-memory type of this map is
     * {@link com.hazelcast.config.InMemoryFormat#BINARY} and the member does not have the class in it's
     * classloader.
     * The downside to this is that map listeners will not receive the old value (since we are performing
     * delete instead of remove).
     */
    public static final String USE_DELETE_WHEN_PROCESSING_REMOVE_EVENTS = "hazelcast.wan.map.useDeleteWhenProcessingRemoveEvents";
    // RU_COMPAT_3_9
    private static final String DEFAULT_LEGACY_MERGE_POLICY = com.hazelcast.map.merge.PassThroughMergePolicy.class.getName();
    private static final String DEFAULT_MERGE_POLICY = PassThroughMergePolicy.class.getName();

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    // RU_COMPAT_3_9
    private final MapMergePolicy defaultSyncLegacyMergePolicy;
    private final SplitBrainMergePolicy defaultSyncMergePolicy;
    private final ProxyService proxyService;
    private final boolean useDeleteWhenProcessingRemoveEvents;

    EnterpriseMapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        // RU_COMPAT_3_9
        MergePolicyProvider mergePolicyProvider = mapServiceContext.getMergePolicyProvider();
        this.defaultSyncLegacyMergePolicy = (MapMergePolicy) mergePolicyProvider.getMergePolicy(DEFAULT_LEGACY_MERGE_POLICY);
        this.defaultSyncMergePolicy = (SplitBrainMergePolicy) mergePolicyProvider.getMergePolicy(DEFAULT_MERGE_POLICY);
        this.proxyService = nodeEngine.getProxyService();
        this.useDeleteWhenProcessingRemoveEvents = Boolean.getBoolean(USE_DELETE_WHEN_PROCESSING_REMOVE_EVENTS);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            String mapName = mapReplicationObject.getMapName();

            /* Proxies should be created to initialize listeners, indexes, etc. and to show WAN replicated maps in mancenter.
             * Otherwise, users are forced to manually call IMap#get()
             * Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
             */
            proxyService.getDistributedObject(MapService.SERVICE_NAME, mapName);

            if (eventObject instanceof EnterpriseMapReplicationSync) {
                handleSyncObject((EnterpriseMapReplicationSync) eventObject);
                return;
            }

            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            if (mapContainer.isWanRepublishingEnabled()) {
                mapContainer.getWanReplicationPublisher().publishReplicationEvent(replicationEvent);
            }

            InternalCompletableFuture completableFuture = null;
            if (eventObject instanceof EnterpriseMapReplicationUpdate) {
                completableFuture = handleUpdate((EnterpriseMapReplicationUpdate) eventObject);
            } else if (eventObject instanceof EnterpriseMapReplicationRemove) {
                completableFuture = handleRemove((EnterpriseMapReplicationRemove) eventObject);
            }
            if (completableFuture != null && replicationEvent.getAcknowledgeType() == ACK_ON_OPERATION_COMPLETE) {
                completableFuture.join();
            }
        }
    }

    private void handleSyncObject(EnterpriseMapReplicationSync syncObject) {
        String mapName = syncObject.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        // RU_COMPAT_3_9
        if (nodeEngine.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V3_10)) {
            SplitBrainMergeEntryView<Data, Data> entryView = createSplitBrainMergeEntryView(syncObject.getEntryView());
            MapOperation operation = operationProvider.createMergeOperation(mapName, entryView, defaultSyncMergePolicy, true);
            invokeOnPartition(entryView.getKey(), operation);
        } else {
            EntryView<Data, Data> entryView = syncObject.getEntryView();
            MapOperation operation = operationProvider.createLegacyMergeOperation(mapName, entryView,
                    defaultSyncLegacyMergePolicy, true);
            invokeOnPartition(entryView.getKey(), operation);
        }
    }

    private InternalCompletableFuture handleUpdate(EnterpriseMapReplicationUpdate replicationUpdate) {
        String mapName = replicationUpdate.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        Object mergePolicy = replicationUpdate.getMergePolicy();

        if (mergePolicy instanceof SplitBrainMergePolicy) {
            SplitBrainMergeEntryView<Data, Data> entryView = createSplitBrainMergeEntryView(replicationUpdate.getEntryView());
            return invokeOnPartition(entryView.getKey(), operationProvider.createMergeOperation(mapName, entryView,
                    (SplitBrainMergePolicy) mergePolicy, true));
        }
        EntryView<Data, Data> entryView = replicationUpdate.getEntryView();
        return invokeOnPartition(entryView.getKey(), operationProvider.createLegacyMergeOperation(mapName, entryView,
                (MapMergePolicy) mergePolicy, true));
    }

    private InternalCompletableFuture handleRemove(EnterpriseMapReplicationRemove replicationRemove) {
        String mapName = replicationRemove.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        MapOperation operation = useDeleteWhenProcessingRemoveEvents
                ? operationProvider.createDeleteOperation(mapName, replicationRemove.getKey(), true)
                : operationProvider.createRemoveOperation(mapName, replicationRemove.getKey(), true);
        return invokeOnPartition(replicationRemove.getKey(), operation);
    }

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
