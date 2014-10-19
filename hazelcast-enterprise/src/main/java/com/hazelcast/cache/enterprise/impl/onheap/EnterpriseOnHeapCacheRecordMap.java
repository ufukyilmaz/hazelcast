package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.impl.record.CacheRecordHashMap;

import java.util.EnumSet;

/**
 * @author sozal 14/10/14
 */
public final class EnterpriseOnHeapCacheRecordMap<K, V>
        extends CacheRecordHashMap<K, V>
        implements com.hazelcast.cache.impl.record.CacheRecordMap<K, V> {

    public EnterpriseOnHeapCacheRecordMap(int initialCapacity) {
        super(initialCapacity);
    }

    public EnterpriseOnHeapCacheRecordMap(int initialCapacity,
                                          float loadFactor,
                                          int concurrencyLevel,
                                          ReferenceType keyType,
                                          ReferenceType valueType,
                                          EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

}
