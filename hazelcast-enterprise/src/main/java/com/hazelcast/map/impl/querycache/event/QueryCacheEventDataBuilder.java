package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

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

    private QueryCacheEventDataBuilder() {
    }

    public static QueryCacheEventDataBuilder newQueryCacheEventDataBuilder() {
        return new QueryCacheEventDataBuilder();
    }

    public QueryCacheEventDataBuilder withDataKey(Data dataKey) {
        this.dataKey = dataKey;
        return this;
    }

    public QueryCacheEventDataBuilder withDataNewValue(Data dataNewValue) {
        this.dataNewValue = dataNewValue;
        return this;
    }

    public QueryCacheEventDataBuilder withDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
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
