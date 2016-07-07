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

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * Enterprise version of {@link MapEventPublisher} helper functionality.
 */
public class EnterpriseMapEventPublisherImpl extends MapEventPublisherImpl {

    public EnterpriseMapEventPublisherImpl(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

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

    private Data toHeapData(Object object) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();

        return serializationService.toData(object, DataType.HEAP);
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
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key), removeTime, mapContainer.getTotalBackupCount());
        if (!isEventFiltered(mapContainer, new SimpleEntryView(key, null), WanFilterEventType.REMOVED)) {
            publishWanReplicationEventInternal(mapName, event);
        }
    }

    @Override
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
                = new EnterpriseMapReplicationRemove(mapName, toHeapData(key), removeTime, mapContainer.getTotalBackupCount());
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

        addEventToQueryCache(eventData);
    }

    @Override
    protected void postPublishEvent(Collection<EntryEventData> eventDataIncludingValues,
                                    Collection<EntryEventData> eventDataExcludingValues) {
        // Publish event data of interest to query caches; since query cache listener registrations
        // include values (as these are required to properly filter according to the query cache's predicate),
        // we do not take into account eventDataExcludingValues, if any were generated.
        if (eventDataIncludingValues != null) {
            for (EntryEventData entryEventData : eventDataIncludingValues) {
                addEventToQueryCache(entryEventData);
            }
        }
    }

    public void addEventToQueryCache(Object eventData) {
        checkInstanceOf(EventData.class, eventData, "eventData");

        String mapName = ((EventData) eventData).getMapName();
        int eventType = ((EventData) eventData).getEventType();

        // in case of expiration, imap publishes both EVICTED and EXPIRED events for a key.
        // Only handling EVICTED event for that key is sufficient.
        if (EXPIRED.getType() == eventType) {
            return;
        }

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
                                                                 Data dataNewValue, Data dataOldValue, int eventTypeId,
                                                                 int partitionId) {
        EventFilter eventFilter = registry.getEventFilter();
        EntryEventType eventType = EntryEventType.getByType(eventTypeId);
        // when using Hazelcast default event filtering strategy, then let the CQC workaround kick-in
        // otherwise just deliver the event if it matches the registry's predicate according to the configured
        // filtering strategy
        if (filteringStrategy instanceof DefaultEntryEventFilteringStrategy) {
            eventType = getCQCEventTypeOrNull(eventType, eventFilter, dataKey, dataNewValue, dataOldValue);
        } else {
            int producedEventTypeId = filteringStrategy.doFilter(eventFilter, dataKey, dataOldValue, dataNewValue,
                    eventType, null);
            if (producedEventTypeId == FilteringStrategy.FILTER_DOES_NOT_MATCH) {
                eventType = null;
            } else {
                eventType = EntryEventType.getByType(producedEventTypeId);
            }
        }
        if (eventType == null) {
            return null;
        }

        boolean includeValue = isIncludeValue(eventFilter);
        return newQueryCacheEventDataBuilder(includeValue)
                .withPartitionId(partitionId)
                .withDataKey(dataKey)
                .withDataNewValue(dataNewValue)
                .withEventType(eventType.getType())
                .withDataOldValue(dataOldValue).build();
    }

    // This method processes UPDATED events and may morph them into ADDED/REMOVED events
    // depending on old/new value matching the EventFilter. Fixes an issue that prevents proper CQC
    // implementation when DefaultEntryEventFilteringStrategy is in use. It is not used when any
    // other filtering strategy is in place.
    private EntryEventType getCQCEventTypeOrNull(EntryEventType eventType, EventFilter eventFilter,
                                                 Data dataKey, Data dataNewValue, Data dataOldValue) {
        boolean newValueMatching = filteringStrategy.doFilter(eventFilter, dataKey, dataOldValue, dataNewValue,
                eventType, null) != FilteringStrategy.FILTER_DOES_NOT_MATCH;
        if (eventType == UPDATED) {
            // UPDATED event has a special handling as it might result in either ADDING or REMOVING an entry from CQC
            // depending on a predicate
            boolean oldValueMatching = filteringStrategy.doFilter(eventFilter, dataKey, null, dataOldValue,
                    EntryEventType.ADDED, null) != FilteringStrategy.FILTER_DOES_NOT_MATCH;
            if (oldValueMatching) {
                if (!newValueMatching) {
                    eventType = REMOVED;
                }
            } else {
                if (newValueMatching) {
                    eventType = ADDED;
                } else {
                    //neither old value or new value is matching -> it's a non-event for the CQC
                    return null;
                }
            }
        } else if (!newValueMatching) {
            return null;
        }
        return eventType;
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

