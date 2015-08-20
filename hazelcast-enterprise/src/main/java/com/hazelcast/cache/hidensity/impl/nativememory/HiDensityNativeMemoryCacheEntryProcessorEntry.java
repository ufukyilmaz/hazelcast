package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;

import javax.cache.expiry.ExpiryPolicy;

/**
 * @param <K> Type of key for entry to be processed
 * @param <V> Type of value for entry to be processed
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V, HiDensityNativeMemoryCacheRecord> {

    private final HiDensityRecordProcessor cacheRecordProcessor;

    public HiDensityNativeMemoryCacheEntryProcessorEntry(
            Data keyData,
            HiDensityNativeMemoryCacheRecord record,
            HiDensityNativeMemoryCacheRecordStore cacheRecordStore, long now, int completionId) {
        super(keyData, record, cacheRecordStore, now, completionId);
        this.cacheRecordProcessor = cacheRecordStore.getRecordProcessor();
    }

    @Override
    protected V getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return (V) ((HiDensityNativeMemoryCacheRecordStore) cacheRecordStore).getRecordValue(record);
    }

    @Override
    public void applyChanges() {
        super.applyChanges();
        if (state != State.CREATE && state != State.LOAD) {
            // If state is neither `CREATE` nor `LOAD`, this means that key is not stored.
            // So, add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
            if (keyData instanceof NativeMemoryData) {
                cacheRecordProcessor.addDeferredDispose((NativeMemoryData) keyData);
            }
        }
    }

    @Override
    protected void onCreate(Data key, Object value, ExpiryPolicy expiryPolicy,
                            long now, boolean disableWriteThrough, int completionId, boolean saved) {
        if (key instanceof NativeMemoryData) {
            if (saved) {
                // If save is successful and key is `NativeMemoryData`, since there is no convertion,
                // add its memory to used memory explicitly.
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            } else {
                // If save is not successful, add key to deferred list so then operation will dispose it later
                cacheRecordProcessor.addDeferredDispose((NativeMemoryData) key);
            }
        }
    }

    @Override
    protected void onLoad(Data key, Object value, ExpiryPolicy expiryPolicy,
                          long now, boolean disableWriteThrough, int completionId, boolean saved) {
        if (key instanceof NativeMemoryData) {
            if (saved) {
                // If save is successful and key is `NativeMemoryData`, since there is no convertion,
                // add its memory to used memory explicitly.
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            } else {
                // If save is not successful, add key to deferred list so then operation will dispose it later
                cacheRecordProcessor.addDeferredDispose((NativeMemoryData) key);
            }
        }
    }

}
