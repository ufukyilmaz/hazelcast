package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Factory for {@link DataQueryCacheRecord}.
 *
 * @see DataQueryCacheRecord
 */
public class DataQueryCacheRecordFactory implements QueryCacheRecordFactory {

    private final SerializationService serializationService;

    public DataQueryCacheRecordFactory(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public QueryCacheRecord createEntry(Data keyData, Data valueData) {
        return new DataQueryCacheRecord(keyData, valueData, serializationService);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        return serializationService.toData(value1).equals(serializationService.toData(value2));
    }

}
