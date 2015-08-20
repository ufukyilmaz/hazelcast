package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.nio.serialization.Data;

/**
 * This handler is used to process event data in {@link SubscriberAccumulator}.
 */
class SubscriberAccumulatorHandler implements AccumulatorHandler<QueryCacheEventData> {

    private final InternalQueryCache queryCache;
    private final boolean includeValue;
    private final SerializationService serializationService;

    public SubscriberAccumulatorHandler(boolean includeValue, InternalQueryCache queryCache,
                                        SerializationService serializationService) {
        this.includeValue = includeValue;
        this.queryCache = queryCache;
        this.serializationService = serializationService;
    }

    @Override
    public void handle(QueryCacheEventData eventData, boolean ignored) {
        eventData.setSerializationService(serializationService);

        Data keyData = eventData.getDataKey();
        Data valueData = includeValue ? eventData.getDataNewValue() : null;

        int eventType = eventData.getEventType();
        EntryEventType entryEventType = EntryEventType.getByType(eventType);
        switch (entryEventType) {
            case ADDED:
            case UPDATED:
                queryCache.setInternal(keyData, valueData, false, entryEventType);
                break;
            case REMOVED:
            case EVICTED:
                queryCache.deleteInternal(keyData, false, entryEventType);
                break;
            // TODO if we want strongly consistent clear & evict, removal can be made based on sequence and partition-id.
            case CLEAR_ALL:
            case EVICT_ALL:
                queryCache.clearInternal(entryEventType);
                break;
            default:
                throw new IllegalArgumentException("Not a known type EntryEventType." + entryEventType);
        }
    }

}




