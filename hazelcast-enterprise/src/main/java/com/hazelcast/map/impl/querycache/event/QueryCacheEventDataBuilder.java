package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

/**
 * Builder for creating a serializable event data for query cache system.
 */
public final class QueryCacheEventDataBuilder {

    private long sequence;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private int eventType;
    private int partitionId;
    private SerializationService serializationService;
    private final boolean includeValue;

    private QueryCacheEventDataBuilder(boolean includeValue) {
        this.includeValue = includeValue;
    }

    public static QueryCacheEventDataBuilder newQueryCacheEventDataBuilder(boolean includeValue) {
        return new QueryCacheEventDataBuilder(includeValue);
    }

    public QueryCacheEventDataBuilder withDataKey(Data dataKey) {
        this.dataKey = dataKey;
        return this;
    }

    public QueryCacheEventDataBuilder withDataNewValue(Data dataNewValue) {
        this.dataNewValue = includeValue ? dataNewValue : null;
        return this;
    }

    public QueryCacheEventDataBuilder withDataOldValue(Data dataOldValue) {
        this.dataOldValue = includeValue ? dataOldValue : null;
        return this;
    }

    public QueryCacheEventDataBuilder withSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public QueryCacheEventDataBuilder withEventType(int eventType) {
        this.eventType = eventType;
        return this;
    }

    public QueryCacheEventDataBuilder withPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public QueryCacheEventDataBuilder withSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
        return this;
    }

    public QueryCacheEventData build() {
        DefaultQueryCacheEventData eventData = new DefaultQueryCacheEventData();
        eventData.setDataKey(dataKey);
        eventData.setDataNewValue(dataNewValue);
        eventData.setDataOldValue(dataOldValue);
        eventData.setSequence(sequence);
        eventData.setSerializationService(serializationService);
        eventData.setEventType(eventType);
        eventData.setPartitionId(partitionId);

        return eventData;
    }
}
