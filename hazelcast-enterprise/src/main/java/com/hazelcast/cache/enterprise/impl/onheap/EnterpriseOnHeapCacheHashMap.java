package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.enterprise.EnterpriseCacheHashMap;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.EnumSet;

/**
 * @author sozal 14/10/14
 */
public final class EnterpriseOnHeapCacheHashMap<K, V>
        extends ConcurrentReferenceHashMap<K, V>
        implements EnterpriseCacheHashMap<K, V> {

    private static final float LOAD_FACTOR = 0.91f;

    public EnterpriseOnHeapCacheHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public EnterpriseOnHeapCacheHashMap(int initialCapacity,
                                        float loadFactor,
                                        int concurrencyLevel,
                                        ConcurrentReferenceHashMap.ReferenceType keyType,
                                        ConcurrentReferenceHashMap.ReferenceType valueType,
                                        EnumSet<ConcurrentReferenceHashMap.Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

}
