package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Represents a record with {@link Data} key and value.
 */
class DataQueryCacheRecord extends AbstractQueryCacheRecord {

    private final Data keyData;

    private final Data valueData;

    private final SerializationService serializationService;

    public DataQueryCacheRecord(Data keyData, Data valueData, SerializationService serializationService) {
        this.keyData = keyData;
        this.valueData = valueData;
        this.serializationService = serializationService;
    }

    @Override
    public Object getValue() {
        return serializationService.toObject(valueData);
    }

    @Override
    public final Data getKey() {
        return keyData;
    }

}
