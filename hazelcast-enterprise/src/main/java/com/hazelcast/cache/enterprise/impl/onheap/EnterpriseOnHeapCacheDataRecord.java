package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public final class EnterpriseOnHeapCacheDataRecord extends EnterpriseOnHeapCacheRecord<Data> {

    public EnterpriseOnHeapCacheDataRecord(Data key, Data value, long expiryTime) {
        super(key, value, expiryTime);
    }

}
