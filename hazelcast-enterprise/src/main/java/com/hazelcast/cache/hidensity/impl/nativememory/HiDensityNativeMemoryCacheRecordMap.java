package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.hidensity.SampleableHiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheEntryIterationResult;
import com.hazelcast.cache.impl.CacheKeyIterationResult;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.util.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sozal 11/02/14
 */
public class HiDensityNativeMemoryCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryCacheRecord>
        implements SampleableHiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    private boolean entryCountingEnable;

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
                                               HiDensityRecordProcessor cacheRecordProcessor,
                                               HiDensityStorageInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor, cacheInfo);
    }

    // Called by only same partition thread. So there is no synchronization and visibility problem.
    @Override
    public void setEntryCounting(boolean enable) {
        if (enable) {
            if (!entryCountingEnable) {
                // It was disable before but now it will be enable.
                // Therefore, we increase the entry count as size of records.
                storageInfo.addEntryCount(size());
            }
        } else {
            if (entryCountingEnable) {
                int size = size();
                // It was enable before but now it will be disable.
                // Therefore, we decrease the entry count as size of records.
                storageInfo.removeEntryCount(size);
            }
        }
        this.entryCountingEnable = enable;
    }

    @Override
    protected void increaseEntryCount() {
        if (entryCountingEnable) {
            super.increaseEntryCount();
        }
    }

    @Override
    protected void decreaseEntryCount() {
        if (entryCountingEnable) {
            super.decreaseEntryCount();
        }
    }

    @Override
    protected void decreaseEntryCount(int entryCount) {
        if (entryCountingEnable) {
            super.decreaseEntryCount(entryCount);
        }
    }

    @Override
    public CacheKeyIterationResult fetchKeys(int nextTableIndex, int size) {
        long now = Clock.currentTimeMillis();
        SlottableIterator<Entry<Data, HiDensityNativeMemoryCacheRecord>> iter = iterator(nextTableIndex);
        List<Data> keys = new ArrayList<Data>(size);
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Entry<Data, HiDensityNativeMemoryCacheRecord> entry = iter.next();
            Data key = entry.getKey();
            HiDensityNativeMemoryCacheRecord record = entry.getValue();
            if (record.isExpiredAt(now)) {
                continue;
            }
            keys.add(recordProcessor.convertData(key, DataType.HEAP));
        }

        return new CacheKeyIterationResult(keys, iter.getNextSlot());
    }

    @Override
    public CacheEntryIterationResult fetchEntries(int nextTableIndex, int size) {
        long now = Clock.currentTimeMillis();
        SlottableIterator<Entry<Data, HiDensityNativeMemoryCacheRecord>> iter = iterator(nextTableIndex);
        List<Entry<Data, Data>> entries = new ArrayList<Entry<Data, Data>>(size);
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Entry<Data, HiDensityNativeMemoryCacheRecord> entry = iter.next();
            Data nativeKey = entry.getKey();
            HiDensityNativeMemoryCacheRecord record = entry.getValue();
            if (record.isExpiredAt(now)) {
                continue;
            }
            NativeMemoryData nativeValue = record.getValue();
            Data key = recordProcessor.convertData(nativeKey, DataType.HEAP);
            Data value = recordProcessor.convertData(nativeValue, DataType.HEAP);
            entries.add(new AbstractMap.SimpleEntry<Data, Data>(key, value));
        }
        return new CacheEntryIterationResult(entries, iter.getNextSlot());
    }


    private final class CacheEvictableSamplingEntry
            extends EvictableSamplingEntry
            implements CacheEntryView {

        private CacheEvictableSamplingEntry(int slot) {
            super(slot);
        }

        @Override
        public long getExpirationTime() {
            return getEntryValue().getExpirationTime();
        }

    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(final int slot) {
        return new CacheEvictableSamplingEntry(slot);
    }

}
