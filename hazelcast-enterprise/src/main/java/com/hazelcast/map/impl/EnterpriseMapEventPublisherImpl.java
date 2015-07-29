package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * Enterprise version of {@link MapEventPublisher} helper functionality.
 */
class EnterpriseMapEventPublisherImpl
        extends MapEventPublisherImpl {

    protected EnterpriseMapEventPublisherImpl(MapServiceContext mapServiceContext) {
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

    @Override
    void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
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
        Result result = applyEventFilter(eventFilter, false, dataKey, dataOldValue, dataNewValue,
                EntryEventType.getByType(eventType));
        boolean includeValue = false;
        switch (result) {
            case VALUE_INCLUDED:
                includeValue = true;
                break;
            case NO_VALUE_INCLUDED:
                break;
            case NONE:
                return null;
            default:
                throw new IllegalArgumentException("Unknown result type " + result);
        }

        return newQueryCacheEventDataBuilder().withPartitionId(partitionId).withDataKey(dataKey)
                                              .withDataNewValue(includeValue ? dataNewValue : null).withEventType(eventType)
                                              .withDataOldValue(includeValue ? dataOldValue : null).build();

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

            QueryCacheEventData singleEventData = newQueryCacheEventDataBuilder().withPartitionId(partitionId)
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

