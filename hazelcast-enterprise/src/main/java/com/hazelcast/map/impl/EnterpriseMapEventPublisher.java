package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.nio.serialization.Data;

/**
 * Enterprise version of {@link MapEventPublisher} helper functionality.
 */
class EnterpriseMapEventPublisher extends MapEventPublisherImpl {

    protected EnterpriseMapEventPublisher(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        EnterpriseMapReplicationUpdate replicationEvent = new EnterpriseMapReplicationUpdate(mapName,
                mapContainer.getWanMergePolicy(), entryView);
        publishWanReplicationEventInternal(mapName, replicationEvent);
    }

    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        final EnterpriseMapReplicationRemove event = new EnterpriseMapReplicationRemove(mapName, key, removeTime);
        publishWanReplicationEventInternal(mapName, event);
    }
}
