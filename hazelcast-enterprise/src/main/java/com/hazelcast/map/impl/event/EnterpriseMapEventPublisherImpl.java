package com.hazelcast.map.impl.event;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.wan.MapFilterProvider;
import com.hazelcast.map.impl.wan.WanEnterpriseMapAddOrUpdateEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapRemoveEvent;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.map.wan.MapWanEventFilter;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.enterprise.wan.WanFilterEventType.LOADED;
import static com.hazelcast.enterprise.wan.WanFilterEventType.UPDATED;

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
    public void publishWanUpdate(@Nonnull String mapName,
                                 @Nonnull WanMapEntryView<Object, Object> entryView,
                                 boolean hasLoadProvenance) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        SplitBrainMergePolicy wanMergePolicy = mapContainer.getWanMergePolicy();
        int totalBackupCount = mapContainer.getTotalBackupCount();
        WanEnterpriseMapAddOrUpdateEvent replicationEvent = new WanEnterpriseMapAddOrUpdateEvent(
                mapName, wanMergePolicy, entryView, totalBackupCount);

        if (!isEventFiltered(mapContainer, entryView, hasLoadProvenance ? LOADED : UPDATED)) {
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
    public void publishWanRemove(@Nonnull String mapName, @Nonnull Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        int totalBackupCount = mapContainer.getTotalBackupCount();
        WanEnterpriseMapRemoveEvent event
                = new WanEnterpriseMapRemoveEvent(mapName, toHeapData(key), totalBackupCount, serializationService);

        WanMapEntryView<Object, Object> entryView = new WanMapEntryView<>(key, null, serializationService);
        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.REMOVED)) {
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
    private boolean isEventFiltered(MapContainer mapContainer,
                                    WanMapEntryView<Object, Object> entryView,
                                    WanFilterEventType eventType) {

        MapConfig mapConfig = mapContainer.getMapConfig();
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        List<String> filters = getFiltersFrom(wanReplicationRef);

        if (filters.isEmpty()) {
            // By default do not transfer updates over WAN if they are the result of
            // loads by MapLoader.
            return eventType == WanFilterEventType.LOADED;
        }

        MapFilterProvider mapFilterProvider = getEnterpriseMapServiceContext().getMapFilterProvider();

        for (String filterName : filters) {
            MapWanEventFilter wanEventFilter = mapFilterProvider.getFilter(filterName);
            if (wanEventFilter.filter(mapContainer.getName(), entryView, eventType)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> getFiltersFrom(WanReplicationRef wanReplicationRef) {
        if (wanReplicationRef == null) {
            return Collections.emptyList();
        }

        List<String> filters = wanReplicationRef.getFilters();
        return CollectionUtil.isEmpty(filters) ? Collections.emptyList() : filters;
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapServiceContext;
    }
}

