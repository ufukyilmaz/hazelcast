package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseOnHeapCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V> {

    public EnterpriseOnHeapCacheEntryProcessorEntry(Data keyData,
                                                    CacheRecord record,
                                                    CacheRecordStore cacheRecordStore,
                                                    long now) {
        super(keyData, record, cacheRecordStore, now);
    }

}
