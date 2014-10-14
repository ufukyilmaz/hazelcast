package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseOnHeapCacheRecordFactory {

    private InMemoryFormat inMemoryFormat;
    private SerializationService serializationService;

    public EnterpriseOnHeapCacheRecordFactory(InMemoryFormat inMemoryFormat,
                                              SerializationService serializationService) {
        this.inMemoryFormat = inMemoryFormat;
        this.serializationService = serializationService;
    }

    public EnterpriseOnHeapCacheRecord newRecord(Data key, Object value) {
        return newRecordWithExpiry(key, value, -1);
    }

    public EnterpriseOnHeapCacheRecord newRecordWithExpiry(Data key, Object value, long expiryTime) {
        final EnterpriseOnHeapCacheRecord record;
        switch (inMemoryFormat) {
            case BINARY:
                Data dataValue = serializationService.toData(value);
                record = new EnterpriseOnHeapCacheDataRecord(key, dataValue, expiryTime);
                break;
            case OBJECT:
                Object objectValue = serializationService.toObject(value);
                record = new EnterpriseOnHeapCacheObjectRecord(key, objectValue, expiryTime);
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + inMemoryFormat);
        }
        return record;
    }

    /**
     * Determines if the Cache Entry associated with this value would be expired
     * at the specified time
     *
     * @param now time in milliseconds (since the Epoc)
     * @return true if the value would be expired at the specified time
     */
    public static boolean isExpiredAt(long expirationTime, long now) {
        return expirationTime > -1 && expirationTime <= now;
    }

}
