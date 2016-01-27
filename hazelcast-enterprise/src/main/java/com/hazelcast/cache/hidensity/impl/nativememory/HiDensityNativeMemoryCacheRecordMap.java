package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.SampleableHiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                assert (size == 0) : "Expected empty cache record map!";
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
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        long now = Clock.currentTimeMillis();
        SlottableIterator<Map.Entry<Data, HiDensityNativeMemoryCacheRecord>> iter =
                iterator(nextTableIndex);
        List<Data> keys = new ArrayList<Data>();
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Map.Entry<Data, HiDensityNativeMemoryCacheRecord> entry = iter.next();
            Data key = entry.getKey();
            HiDensityNativeMemoryCacheRecord record = entry.getValue();
            if (record.isExpiredAt(now)) {
                continue;
            }
            keys.add(recordProcessor.convertData(key, DataType.HEAP));
        }

        return new CacheKeyIteratorResult(keys, iter.getNextSlot());
    }

}
