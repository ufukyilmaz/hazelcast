package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseOnHeapCacheRecordFactory extends CacheRecordFactory {

    public EnterpriseOnHeapCacheRecordFactory(InMemoryFormat inMemoryFormat,
                                              SerializationService serializationService) {
        super(inMemoryFormat, serializationService);
    }

    @Override
    protected CacheRecord createCacheDataRecord(Data key, Data dataValue, long expiryTime) {
        return new EnterpriseOnHeapCacheDataRecord(key, dataValue, expiryTime);
    }

    @Override
    protected CacheRecord createCacheObjectRecord(Data key, Object objectValue, long expiryTime) {
        return new EnterpriseOnHeapCacheObjectRecord(key, objectValue, expiryTime);
    }

}
