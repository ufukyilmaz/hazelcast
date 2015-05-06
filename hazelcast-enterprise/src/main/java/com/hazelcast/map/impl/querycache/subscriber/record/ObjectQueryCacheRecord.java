package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Represents a record with {@link Data} key and {@link Object} value.
 */
class ObjectQueryCacheRecord extends AbstractQueryCacheRecord {

    private final Data keyData;

    private final Object value;

    public ObjectQueryCacheRecord(Data keyData, Data valueData, SerializationService serializationService) {
        this.keyData = keyData;
        this.value = serializationService.toObject(valueData);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public final Data getKey() {
        return keyData;
    }

}
