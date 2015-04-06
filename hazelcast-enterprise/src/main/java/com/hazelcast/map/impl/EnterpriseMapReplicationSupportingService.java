package com.hazelcast.map.impl;

import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.operation.WanOriginatedDeleteOperation;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.concurrent.Future;

/**
 * This class handles incoming map WAN replication events
 */

class EnterpriseMapReplicationSupportingService implements ReplicationSupportingService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public EnterpriseMapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();

        if (eventObject instanceof EnterpriseMapReplicationObject) {
            EnterpriseMapReplicationObject mapReplicationObject = (EnterpriseMapReplicationObject) eventObject;
            String mapName = mapReplicationObject.getMapName();
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            WanReplicationRef wanReplicationRef = mapContainer.getMapConfig().getWanReplicationRef();

            if (wanReplicationRef !=  null && wanReplicationRef.isRepublishingEnabled()) {
                WanReplicationPublisher wanPublisher = mapContainer.getWanReplicationPublisher();
                if (wanPublisher != null) {
                    wanPublisher.publishReplicationEvent(MapService.SERVICE_NAME, mapReplicationObject);
                }
            }
            if (eventObject instanceof EnterpriseMapReplicationUpdate) {
                EnterpriseMapReplicationUpdate replicationUpdate = (EnterpriseMapReplicationUpdate) eventObject;
                EntryView<Data, Data> entryView = replicationUpdate.getEntryView();
                MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
                MergeOperation operation = new MergeOperation(mapName, mapServiceContext.toData(entryView.getKey(),
                        mapContainer.getPartitioningStrategy()), entryView, mergePolicy);
                invokeOnPartition(entryView.getKey(), operation);
            } else if (eventObject instanceof EnterpriseMapReplicationRemove) {
                EnterpriseMapReplicationRemove replicationRemove = (EnterpriseMapReplicationRemove) eventObject;
                WanOriginatedDeleteOperation operation = new WanOriginatedDeleteOperation(mapName,
                        replicationRemove.getKey());
                invokeOnPartition(replicationRemove.getKey(), operation);
            }
        }
    }

    private void invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(mapServiceContext.serviceName(), operation, partitionId);
            f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
