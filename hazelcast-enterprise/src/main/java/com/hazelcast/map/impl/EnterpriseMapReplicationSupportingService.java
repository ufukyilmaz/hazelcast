package com.hazelcast.map.impl;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

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
    private static final String DEFAULT_MERGE_POLICY = PassThroughMergePolicy.class.getName();

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final MapMergePolicy defaultSyncMergePolicy;
    private final ProxyService proxyService;
    private final boolean useDeleteWhenProcessingRemoveEvents;

    EnterpriseMapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.defaultSyncMergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(DEFAULT_MERGE_POLICY);
        this.proxyService = nodeEngine.getProxyService();
        this.useDeleteWhenProcessingRemoveEvents = Boolean.getBoolean(USE_DELETE_WHEN_PROCESSING_REMOVE_EVENTS);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof EnterpriseMapReplicationObject) {

            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            String mapName = mapReplicationObject.getMapName();

            /** Proxies should be created to initialize listeners, indexes, etc. and to show WAN replicated maps in mancenter.
             * Otherwise, users are forced to manually call IMap#get()
             * Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
             */
            proxyService.getDistributedObject(MapService.SERVICE_NAME, mapName);

            if (eventObject instanceof EnterpriseMapReplicationSync) {
                handleSyncObject((EnterpriseMapReplicationSync) eventObject);
                return;
            }

            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            WanReplicationRef wanReplicationRef = mapContainer.getMapConfig().getWanReplicationRef();

            if (wanReplicationRef != null && wanReplicationRef.isRepublishingEnabled()) {
                WanReplicationPublisher wanPublisher = mapContainer.getWanReplicationPublisher();
                if (wanPublisher != null) {
                    wanPublisher.publishReplicationEvent(replicationEvent);
                }
            }
            InternalCompletableFuture completableFuture = null;
            MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

            if (eventObject instanceof EnterpriseMapReplicationUpdate) {
                EnterpriseMapReplicationUpdate replicationUpdate = (EnterpriseMapReplicationUpdate) eventObject;
                EntryView<Data, Data> entryView = replicationUpdate.getEntryView();
                MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();

                MapOperation operation = operationProvider.createMergeOperation(mapName, entryView, mergePolicy, true);
                completableFuture = invokeOnPartition(entryView.getKey(), operation);
            } else if (eventObject instanceof EnterpriseMapReplicationRemove) {
                EnterpriseMapReplicationRemove replicationRemove = (EnterpriseMapReplicationRemove) eventObject;

                MapOperation operation = useDeleteWhenProcessingRemoveEvents
                        ? operationProvider.createDeleteOperation(
                        replicationRemove.getMapName(), replicationRemove.getKey(), true)
                        : operationProvider.createRemoveOperation(
                        replicationRemove.getMapName(), replicationRemove.getKey(), true);
                completableFuture = invokeOnPartition(replicationRemove.getKey(), operation);
            }

            if (completableFuture != null
                    && replicationEvent.getAcknowledgeType() == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
                completableFuture.join();
            }
        }
    }

    private void handleSyncObject(EnterpriseMapReplicationSync syncObject) {
        EntryView<Data, Data> entryView = syncObject.getEntryView();
        String mapName = syncObject.getMapName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapOperation operation
                = operationProvider.createMergeOperation(mapName, entryView, defaultSyncMergePolicy, true);
        invokeOnPartition(entryView.getKey(), operation);
    }

    private InternalCompletableFuture invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
