package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Builder for creating a serializable event data for query cache system.
 */
public final class SingleEventDataBuilder {

    private long sequence;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private int eventType;
    private int partitionId;
    private SerializationService serializationService;

    private SingleEventDataBuilder() {
    }

    public static SingleEventDataBuilder newSingleEventDataBuilder() {
        return new SingleEventDataBuilder();
    }

    public SingleEventDataBuilder withDataKey(Data dataKey) {
        this.dataKey = dataKey;
        return this;
    }

    public SingleEventDataBuilder withDataNewValue(Data dataNewValue) {
        this.dataNewValue = dataNewValue;
        return this;
    }

    public SingleEventDataBuilder withDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
        return this;
    }

    public SingleEventDataBuilder withSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public SingleEventDataBuilder withEventType(int eventType) {
        this.eventType = eventType;
        return this;
    }

    public SingleEventDataBuilder withPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public SingleEventDataBuilder withSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
        return this;
    }

    public SingleEventData build() {
        DefaultSingleEventData eventData = new DefaultSingleEventData();
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
