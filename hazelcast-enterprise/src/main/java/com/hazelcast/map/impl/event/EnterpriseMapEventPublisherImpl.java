package com.hazelcast.map.impl.event;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.impl.wan.MapFilterProvider;
import com.hazelcast.map.wan.MapWanEventFilter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.CollectionUtil;

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
    public void publishWanUpdate(String mapName, EntryView<Data, Data> entryView, boolean hasLoadProvenance) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Object wanMergePolicy = mapContainer.getWanMergePolicy();
        int totalBackupCount = mapContainer.getTotalBackupCount();
        EnterpriseMapReplicationUpdate replicationEvent
                = new EnterpriseMapReplicationUpdate(mapName, wanMergePolicy, entryView, totalBackupCount);

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
    public void publishWanRemove(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        int totalBackupCount = mapContainer.getTotalBackupCount();
        EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key), totalBackupCount);

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
    private boolean isEventFiltered(MapContainer mapContainer,
                                    EntryView<Data, Data> entryView,
                                    WanFilterEventType eventType) {

        MapConfig mapConfig = mapContainer.getMapConfig();
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        List<String> filters = getFiltersFrom(wanReplicationRef);

        if (filters.isEmpty()) {
            // By default do not transfer updates over WAN if they are the result of
            // loads by MapLoader.
            return eventType == WanFilterEventType.LOADED;
        }

        EntryView lazyEntryView = toLazyEntryView(entryView, serializationService);
        MapFilterProvider mapFilterProvider = getEnterpriseMapServiceContext().getMapFilterProvider();

        for (String filterName : filters) {
            MapWanEventFilter wanEventFilter = mapFilterProvider.getFilter(filterName);
            if (wanEventFilter.filter(mapContainer.getName(), lazyEntryView, eventType)) {
                return true;
            }
        }
        return false;
    }

    public static <K, V> EntryView<K, V> toLazyEntryView(EntryView<K, V> entryView,
                                                         SerializationService serializationService) {
        return new LazyEntryView<>(entryView.getKey(), entryView.getValue(), serializationService)
                .setCost(entryView.getCost())
                .setVersion(entryView.getVersion())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setTtl(entryView.getTtl())
                .setMaxIdle(entryView.getMaxIdle())
                .setCreationTime(entryView.getCreationTime())
                .setHits(entryView.getHits())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastStoredTime(entryView.getLastStoredTime());
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

