package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public final class EnterpriseOnHeapCacheObjectRecord extends EnterpriseOnHeapCacheRecord<Object> {

    public EnterpriseOnHeapCacheObjectRecord(Data key, Object value, long expiryTime) {
        super(key, value, expiryTime);
    }

}
