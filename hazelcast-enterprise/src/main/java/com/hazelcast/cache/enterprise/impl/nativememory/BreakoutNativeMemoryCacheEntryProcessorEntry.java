package com.hazelcast.cache.enterprise.impl.nativememory;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.nio.serialization.Data;

/**
 * @param <K> Type of key for entry to be processed
 * @param <V> Type of value for entry to be processed
 * @author sozal 14/10/14
 */
public class BreakoutNativeMemoryCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V, BreakoutNativeMemoryCacheRecord> {

    public BreakoutNativeMemoryCacheEntryProcessorEntry(Data keyData, BreakoutNativeMemoryCacheRecord record,
            BreakoutNativeMemoryCacheRecordStore cacheRecordStore, long now) {
        super(keyData, record, cacheRecordStore, now);
    }

    @Override
    protected V getRecordValue(BreakoutNativeMemoryCacheRecord record) {
        return (V) ((BreakoutNativeMemoryCacheRecordStore) cacheRecordStore).getRecordValue(record);
    }

}
