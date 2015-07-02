package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.SampleableHiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author sozal 11/02/14
 */
public final class HiDensityNativeMemoryCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryCacheRecord>
        implements SampleableHiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
                                               HiDensityRecordProcessor cacheRecordProcessor,
                                               Callback<Data> evictionCallback,
                                               HiDensityStorageInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor, evictionCallback, cacheInfo);
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
            final boolean isExpired = record.isExpiredAt(now);
            if (!isExpired) {
                keys.add(recordProcessor.convertData(key, DataType.HEAP));
            }
        }

        return new CacheKeyIteratorResult(keys, iter.getNextSlot());
    }

    //CHECKSTYLE:OFF
    @Override
    public int forceEvict(int evictionPercentage) {
        if (evictionPercentage <= 0) {
            return 0;
        }
        int size = size();
        if (size == 0) {
            return 0;
        }

        if (evictionPercentage >= ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        int len = (int) (size * (long) evictionPercentage / ONE_HUNDRED_PERCENT);
        len = Math.max(len, MIN_EVICTION_ELEMENT_COUNT);

        final int start = evictionLastIndex;
        final int end = capacity();
        final int mask = end - 1;
        // assert capacity is power of 2, otherwise loop below will not work...
        // we know BinaryOffHeapHashMap.capacity() is power of 2
        assert QuickMath.isPowerOfTwo(end);

        int ix = start;
        int k = 0;
        while (true) {
            if (isAllocated(ix)) {
                long key = getKey(ix);
                NativeMemoryData keyData = recordProcessor.readData(key);
                delete(keyData);
                recordProcessor.disposeData(keyData);
                if (++k >= len) {
                    break;
                }
            }
            ix = (ix + 1) & mask;
            if (ix == start) {
                break;
            }
        }
        evictionLastIndex = ix;
        return k;
    }
    //CHECKSTYLE:ON

}
