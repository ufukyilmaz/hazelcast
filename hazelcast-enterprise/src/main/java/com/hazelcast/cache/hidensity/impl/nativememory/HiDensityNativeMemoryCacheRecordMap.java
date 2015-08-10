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
public final class HiDensityNativeMemoryCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryCacheRecord>
        implements SampleableHiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
                                               HiDensityRecordProcessor cacheRecordProcessor,
                                               HiDensityStorageInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor, cacheInfo);
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

}
