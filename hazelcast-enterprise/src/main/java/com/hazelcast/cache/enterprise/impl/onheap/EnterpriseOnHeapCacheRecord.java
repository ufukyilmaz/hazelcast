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

    protected V value;

    public EnterpriseOnHeapCacheRecord(Data key, V value, long expiryTime) {
        super(expiryTime);
        this.value = value;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

}
