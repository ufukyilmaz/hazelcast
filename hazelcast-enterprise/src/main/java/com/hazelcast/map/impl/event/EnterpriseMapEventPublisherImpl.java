package com.hazelcast.map.impl.event;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.wan.filter.MapWanEventFilter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.List;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Enterprise version of {@link MapEventPublisher} helper functionality.
 */
public class EnterpriseMapEventPublisherImpl extends MapEventPublisherImpl {

    public EnterpriseMapEventPublisherImpl(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    /**
     * {@inheritDoc}
     * If the in-memory format for this map is {@link InMemoryFormat#NATIVE} it will copy the values on-heap
     * before publishing the event.
     */
    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType, Data dataKey, Object oldValue,
                             Object value, Object mergingValue) {

        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            dataKey = toHeapData(dataKey);
            oldValue = toHeapData(oldValue);
            value = toHeapData(value);
            mergingValue = toHeapData(mergingValue);
        }
        super.publishEvent(caller, mapName, eventType, dataKey, oldValue, value, mergingValue);
    }

    /**
     * Transforms the {@code object} from whichever format to an on-heap {@link Data} object
     * and returns it.
     */
    private Data toHeapData(Object object) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();

        return serializationService.toData(object, DataType.HEAP);
    }

    /**
     * {@inheritDoc}
     * Additionally filters events and publishes only events that do not match the {@link MapWanEventFilter}
     * configured for this map.
     *
     * @param mapName   the map name
     * @param entryView the updated entry
     */
    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView<Data, Data> entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        EnterpriseMapReplicationUpdate replicationEvent = new EnterpriseMapReplicationUpdate(mapName,
                mapContainer.getWanMergePolicy(), entryView, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.UPDATED)) {
            publishWanReplicationEventInternal(mapName, replicationEvent);
        }
    }

    /**
     * {@inheritDoc}
     * Additionally filters events and publishes only events that do not match the {@link MapWanEventFilter}
     * configured for this map.
     *
     * @param mapName    the map name
     * @param key        the key of the removed entry
     * @param removeTime the clock time for the remove event
     */
    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key), removeTime, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanReplicationEventInternal(mapName, event);
        }
    }

    /**
     * {@inheritDoc}
     * Additionally filters events and publishes only events that do not match the {@link MapWanEventFilter}
     * configured for this map.
     *
     * @param mapName   the map name
     * @param entryView the updated entry
     */
    @Override
    public void publishWanReplicationUpdateBackup(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        EnterpriseMapReplicationUpdate replicationEvent = new EnterpriseMapReplicationUpdate(mapName,
                mapContainer.getWanMergePolicy(), entryView, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.UPDATED)) {
            publishWanReplicationEventInternalBackup(mapName, replicationEvent);
        }
    }

    /**
     * {@inheritDoc}
     * Additionally filters events and publishes only events that do not match the {@link MapWanEventFilter}
     * configured for this map.
     *
     * @param mapName    the map name
     * @param key        the key of the removed entry
     * @param removeTime the clock time for the remove event
     */
    @Override
    public void publishWanReplicationRemoveBackup(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key), removeTime, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanReplicationEventInternalBackup(mapName, event);
        }
    }

    /**
     * Returns {@code true} if there is a {@link MapWanEventFilter} configured for this map and the
     * entry and event type match.
     *
     * @param mapContainer the map container
     * @param entryView    the entry for the map event
     * @param eventType    the event type
     * @return if the event matches the WAN replication filter
     */
    private boolean isEventFiltered(MapContainer mapContainer, EntryView entryView, WanFilterEventType eventType) {
        List<String> filters = mapContainer.getMapConfig().getWanReplicationRef().getFilters();
        if (!filters.isEmpty()) {
            entryView = EntryViews.convertToLazyEntryView(entryView, serializationService, null);
            for (String filterName : filters) {
                MapWanEventFilter mapWanEventFilter =
                        getEnterpriseMapServiceContext().getMapFilterProvider().getFilter(filterName);
                if (mapWanEventFilter.filter(mapContainer.getName(), entryView, eventType)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Publishes a backup {@code event} to the {@link WanReplicationPublisher} configured for this map.
     *
     * @param mapName the map name
     * @param event   the event
     */
    void publishWanReplicationEventInternalBackup(String mapName, ReplicationEventObject event) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final WanReplicationPublisher wanReplicationPublisher = mapContainer.getWanReplicationPublisher();
        wanReplicationPublisher.publishReplicationEventBackup(SERVICE_NAME, event);
    }

    public EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapServiceContext;
    }
}

