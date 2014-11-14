package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.nio.serialization.Data;

/**
 * @param <K> Type of key for entry to be processed
 * @param <V> Type of value for entry to be processed
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V, HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheEntryProcessorEntry(Data keyData,
            HiDensityNativeMemoryCacheRecord record,
            HiDensityNativeMemoryCacheRecordStore cacheRecordStore, long now, int completionId) {
        super(keyData, record, cacheRecordStore, now, completionId);
    }

    @Override
    protected V getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return (V) ((HiDensityNativeMemoryCacheRecordStore) cacheRecordStore).getRecordValue(record);
    }

}
