package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data; 
/**
 * Factory for {@link ObjectQueryCacheRecord}.
 *
 * @see ObjectQueryCacheRecord
 */
public class ObjectQueryCacheRecordFactory implements QueryCacheRecordFactory {

    private final SerializationService serializationService;

    public ObjectQueryCacheRecordFactory(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public QueryCacheRecord createEntry(Data keyData, Data valueData) {
        return new ObjectQueryCacheRecord(keyData, valueData, serializationService);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        Object v1 = value1 instanceof Data ? serializationService.toObject(value1) : value1;
        Object v2 = value2 instanceof Data ? serializationService.toObject(value2) : value2;
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null) {
            return false;
        }
        if (v2 == null) {
            return false;
        }
        return v1.equals(v2);
    }
}
