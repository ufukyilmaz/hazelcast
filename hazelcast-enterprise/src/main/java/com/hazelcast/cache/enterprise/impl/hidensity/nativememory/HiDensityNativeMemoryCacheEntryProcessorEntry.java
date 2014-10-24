package com.hazelcast.cache.enterprise.impl.hidensity.nativememory;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V, HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheEntryProcessorEntry(Data keyData,
                                                         HiDensityNativeMemoryCacheRecord record,
                                                         HiDensityNativeMemoryCacheRecordStore cacheRecordStore,
                                                         long now) {
        super(keyData, record, cacheRecordStore, now);
    }

    @Override
    protected V getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return (V) ((HiDensityNativeMemoryCacheRecordStore)cacheRecordStore).getRecordValue(record);
    }

}
