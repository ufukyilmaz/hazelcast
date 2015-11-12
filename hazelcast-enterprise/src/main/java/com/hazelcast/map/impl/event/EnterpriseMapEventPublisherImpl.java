package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.map.wan.filter.MapWanEventFilter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * Enterprise version of {@link MapEventPublisher} helper functionality.
 */
public class EnterpriseMapEventPublisherImpl
        extends MapEventPublisherImpl {

    public EnterpriseMapEventPublisherImpl(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean syntheticEvent,
                             Data dataKey, Data dataOldValue, Data dataValue, Data dataMergingValue) {

        dataKey = toHeapData(dataKey);
        dataOldValue = toHeapData(dataOldValue);
        dataValue = toHeapData(dataValue);
        dataMergingValue = toHeapData(dataMergingValue);

        super.publishEvent(caller, mapName, eventType, syntheticEvent, dataKey, dataOldValue, dataValue, dataMergingValue);
    }

    private Data toHeapData(Data dataKey) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();

        return serializationService.toData(dataKey, DataType.HEAP);
    }

    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        EnterpriseMapReplicationUpdate replicationEvent = new EnterpriseMapReplicationUpdate(mapName,
                mapContainer.getWanMergePolicy(), entryView, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.UPDATED)) {
            publishWanReplicationEventInternal(mapName, replicationEvent);
        }
    }

    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, key, removeTime, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanReplicationEventInternal(mapName, event);
        }
    }

    public void publishWanReplicationUpdateBackup(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        EnterpriseMapReplicationUpdate replicationEvent = new EnterpriseMapReplicationUpdate(mapName,
                mapContainer.getWanMergePolicy(), entryView, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, entryView, WanFilterEventType.UPDATED)) {
            publishWanReplicationEventInternalBackup(mapName, replicationEvent);
        }
    }

    @Override
    public void publishWanReplicationRemoveBackup(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final EnterpriseMapReplicationRemove event
                = new EnterpriseMapReplicationRemove(mapName, key, removeTime, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanReplicationEventInternalBackup(mapName, event);
        }
    }

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

    void publishWanReplicationEventInternalBackup(String mapName, ReplicationEventObject event) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final WanReplicationPublisher wanReplicationPublisher = mapContainer.getWanReplicationPublisher();
        wanReplicationPublisher.publishReplicationEventBackup(SERVICE_NAME, event);
    }

    @Override
    protected void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
        super.publishEventInternal(registrations, eventData, orderKey);
        checkInstanceOf(EventData.class, eventData, "eventData");

        String mapName = ((EventData) eventData).getMapName();
        int eventType = ((EventData) eventData).getEventType();

        // this collection contains all defined query-caches on an IMap.
        Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = getPartitionAccumulatorRegistries(mapName);
        if (isEmpty(partitionAccumulatorRegistries)) {
            return;
        }

        if (!(eventData instanceof EntryEventData)) {
            return;
        }

        EntryEventData entryEvenData = (EntryEventData) eventData;
        Data dataKey = entryEvenData.getDataKey();
        Data dataNewValue = entryEvenData.getDataNewValue();
        Data dataOldValue = entryEvenData.getDataOldValue();
        QueryCacheContext queryCacheContext = getQueryCacheContext();
        int partitionId = queryCacheContext.getPartitionId(entryEvenData.dataKey);

        for (PartitionAccumulatorRegistry registry : partitionAccumulatorRegistries) {
            DefaultQueryCacheEventData singleEventData = (DefaultQueryCacheEventData) convertQueryCacheEventDataOrNull(registry,
                    dataKey, dataNewValue, dataOldValue, eventType, partitionId);

            if (singleEventData == null) {
                continue;
            }

            Accumulator accumulator = registry.getOrCreate(partitionId);
            accumulator.accumulate(singleEventData);
        }
    }

    private QueryCacheEventData convertQueryCacheEventDataOrNull(PartitionAccumulatorRegistry registry, Data dataKey,
                                                                 Data dataNewValue, Data dataOldValue, int eventType,
                                                                 int partitionId) {
        EventFilter eventFilter = registry.getEventFilter();
        // TODO handle synthetic events when applying filter.
        if (!doFilter(eventFilter, false, dataKey, dataOldValue, dataNewValue,
                EntryEventType.getByType(eventType), null)) {
            return null;
        }

        boolean includeValue = isIncludeValue(eventFilter);
        return newQueryCacheEventDataBuilder(includeValue)
                .withPartitionId(partitionId)
                .withDataKey(dataKey)
                .withDataNewValue(dataNewValue)
                .withEventType(eventType)
                .withDataOldValue(dataOldValue).build();
    }

    // TODO Problem : Locked keys will also be cleared from the query-cache after calling a map-wide event like clear/evictAll.
    @Override
    public void hintMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected,
                             int partitionId) {
        super.hintMapEvent(caller, mapName, eventType, numberOfEntriesAffected, partitionId);
        // this collection contains all defined query-caches on this map.
        Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = getPartitionAccumulatorRegistries(mapName);
        for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
            Accumulator accumulator = accumulatorRegistry.getOrCreate(partitionId);

            QueryCacheEventData singleEventData = newQueryCacheEventDataBuilder(false).withPartitionId(partitionId)
                    .withEventType(eventType.getType()).build();

            accumulator.accumulate(singleEventData);
        }
    }

    private Collection<PartitionAccumulatorRegistry> getPartitionAccumulatorRegistries(String mapName) {
        QueryCacheContext queryCacheContext = getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return Collections.emptySet();
        }
        // this collection contains all query-caches for this map.
        return publisherRegistry.getAll().values();
    }

    private QueryCacheContext getQueryCacheContext() {
        return getEnterpriseMapServiceContext().getQueryCacheContext();
    }

    public EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapServiceContext;
    }
}

