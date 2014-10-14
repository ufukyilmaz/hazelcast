package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.enterprise.EnterpriseCacheRecord;
import com.hazelcast.cache.impl.record.AbstractCacheRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseOnHeapCacheRecord<V>
        extends AbstractCacheRecord<V>
        implements EnterpriseCacheRecord<V> {

    public EnterpriseOnHeapCacheRecord(Data key, V value, long expiryTime) {
        super(key, value, expiryTime);
    }

}
