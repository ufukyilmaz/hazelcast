package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecordSortArea;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author sozal 11/02/14
 */
public final class HiDensityNativeMemoryCacheRecordMap
        extends BinaryElasticHashMap<HiDensityNativeMemoryCacheRecord>
        implements HiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    private static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    private final transient HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor;
    private final transient Callback<Data> evictionCallback;
    private int randomEvictionLastIndex;
    private final Random random = new Random();

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
            EnterpriseSerializationService serializationService,
            HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor,
            Callback<Data> evictionCallback) {
        super(initialCapacity, serializationService, cacheRecordAccessor,
                serializationService.getMemoryManager().unwrapMemoryAllocator());
        this.cacheRecordAccessor = cacheRecordAccessor;
        this.evictionCallback = evictionCallback;
    }

    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return new NativeOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
    }

    //CHECKSTYLE:OFF
    @Override
    public int evictExpiredRecords(int percentage) {
        int capacity = capacity();
        int len = (int) (capacity * (long) percentage / ICacheRecordStore.ONE_HUNDRED_PERCENT);
        int k = 0;
        if (len > 0 && size() > 0) {
            int start = percentage < ICacheRecordStore.ONE_HUNDRED_PERCENT ? (int) (random.nextInt(capacity)) : 0;
            int end = percentage < ICacheRecordStore.ONE_HUNDRED_PERCENT ? Math.min(start + len, capacity) : capacity;

            long now = Clock.currentTimeMillis();
            for (int ix = start; ix < end; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    int ttlMillis = HiDensityNativeMemoryCacheRecord.getTtlMillis(value);
                    if (ttlMillis > 0) {
                        long creationTime = HiDensityNativeMemoryCacheRecord.getCreationTime(value);
                        if (creationTime + ttlMillis < now) {
                            long key = getKey(ix);
                            NativeMemoryData binary = cacheRecordAccessor.readData(key);
                            callbackEvictionListeners(binary);
                            delete(binary);
                            cacheRecordAccessor.disposeData(binary);
                            k++;
                        }
                    }
                }
            }
        }
        return k;
    }
    //CHECKSTYLE:ON

    private void callbackEvictionListeners(NativeMemoryData binary) {
        if (evictionCallback != null) {
            evictionCallback.notify(binary);
        }
    }

    private long getAccessTime(long recordAddress) {
        long creationTime = HiDensityNativeMemoryCacheRecord.getCreationTime(recordAddress);
        int accessTimeDiff = HiDensityNativeMemoryCacheRecord.getAccessTimeDiff(recordAddress);
        return creationTime + accessTimeDiff;
    }

    @Override
    public int evictRecords(int percentage, EvictionPolicy policy) {
        switch (policy) {
            case RANDOM:
                return evictRecordsRandom(percentage);

            case LRU:
                try {
                    return evictRecordsLRU(percentage);
                } catch (Throwable e) {
                    return evictRecordsRandom(percentage);
                }

            case LFU:
                try {
                    return evictRecordsLFU(percentage);
                } catch (Throwable e) {
                    return evictRecordsRandom(percentage);
                }

            default:
                throw new IllegalArgumentException();
        }
    }

    //CHECKSTYLE:OFF
    private int evictRecordsLRU(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }
        final int capacity = capacity();
        int k = 0;
        if (size > 0) {
            long[] sortArray = CacheRecordSortArea.SORT_AREA_THREAD_LOCAL.get().getLongArray(size);
            for (int ix = 0; ix < capacity; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    if (value <= 0L) {
                        new AssertionError("VALUE ==== " + value).printStackTrace();
                    }
                    long accessTime = getAccessTime(value);
                    sortArray[k] = accessTime;
                    if (++k >= size) {
                        break;
                    }
                }
            }

            Arrays.sort(sortArray, 0, size);
            int index = (int) (size * (long) percentage / ICacheRecordStore.ONE_HUNDRED_PERCENT);
            long time = sortArray[index];

            k = 0;
            for (int ix = 0; ix < capacity && k < index; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    long accessTime = getAccessTime(value);
                    if (accessTime <= time) {
                        k++;
                        long key = getKey(ix);
                        NativeMemoryData keyData = cacheRecordAccessor.readData(key);
                        delete(keyData);
                        cacheRecordAccessor.disposeData(keyData);
                    }
                }
            }
        }
        return k;
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    private int evictRecordsLFU(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }
        final int capacity = capacity();
        int k = 0;
        if (size > 0) {
            int hit;
            int[] sortArray = CacheRecordSortArea.SORT_AREA_THREAD_LOCAL.get().getIntArray(size);
            for (int ix = 0; ix < capacity; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    hit = HiDensityNativeMemoryCacheRecord.getAccessHit(value);
                    sortArray[k] = hit;
                    if (++k >= size) {
                        break;
                    }
                }
            }
            Arrays.sort(sortArray, 0, size);
            int index = (int) (size * (long) percentage / ICacheRecordStore.ONE_HUNDRED_PERCENT);
            hit = sortArray[index];

            k = 0;
            for (int ix = 0; ix < capacity && k < index; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    int h = HiDensityNativeMemoryCacheRecord.getAccessHit(value);
                    if (h <= hit) {
                        k++;
                        long key = getKey(ix);
                        NativeMemoryData keyData = cacheRecordAccessor.readData(key);
                        delete(keyData);
                        cacheRecordAccessor.disposeData(keyData);
                    }
                }
            }
        }
        return k;
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    private int evictRecordsRandom(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        int size = size();
        if (size == 0) {
            return 0;
        }

        if (percentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        int len = (int) (size * (long) percentage / ICacheRecordStore.ONE_HUNDRED_PERCENT);
        len = Math.max(len, MIN_EVICTION_ELEMENT_COUNT);

        final int start = randomEvictionLastIndex;
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
                NativeMemoryData keyData = cacheRecordAccessor.readData(key);
                delete(keyData);
                cacheRecordAccessor.disposeData(keyData);
                if (++k >= len) {
                    break;
                }
            }
            ix = (ix + 1) & mask;
            if (ix == start) {
                break;
            }
        }
        randomEvictionLastIndex = ix;
        return k;
    }
    //CHECKSTYLE:ON

    @Override
    public EntryIter iterator(int slot) {
        return new EntryIter(slot);
    }

    @Override
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        long now = Clock.currentTimeMillis();
        BinaryElasticHashMap<HiDensityNativeMemoryCacheRecord>.EntryIter iter =
                iterator(nextTableIndex);
        List<Data> keys = new ArrayList<Data>();
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Map.Entry<Data, HiDensityNativeMemoryCacheRecord> entry = iter.next();
            Data key = entry.getKey();
            HiDensityNativeMemoryCacheRecord record = entry.getValue();
            final boolean isExpired = record.isExpiredAt(now);
            if (!isExpired) {
                keys.add(serializationService.convertData(key, DataType.HEAP));
            }
        }
        return new CacheKeyIteratorResult(keys, iter.getNextSlot());
    }

}
