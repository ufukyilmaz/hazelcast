package com.hazelcast.map.impl.event;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.wan.filter.MapWanEventFilter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.List;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

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
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Object oldValue,
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
    public void publishWanUpdate(String mapName, EntryView<Data, Data> entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Object wanMergePolicy = mapContainer.getWanMergePolicy();
        int totalBackupCount = mapContainer.getTotalBackupCount();
        EnterpriseMapReplicationUpdate replicationEvent
                = new EnterpriseMapReplicationUpdate(mapName, wanMergePolicy, entryView, totalBackupCount);

        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.UPDATED)) {
            publishWanEvent(mapName, replicationEvent);
        }
    }

    /**
     * {@inheritDoc}
     * Additionally filters events and publishes only events that do not match the {@link MapWanEventFilter}
     * configured for this map.
     *
     * @param mapName the map name
     * @param key     the key of the removed entry
     */
    @Override
    public void publishWanRemove(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        int totalBackupCount = mapContainer.getTotalBackupCount();
        EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key),
                Clock.currentTimeMillis(), totalBackupCount);

        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanEvent(mapName, event);
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

    public EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapServiceContext;
    }
}

