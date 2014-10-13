package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.AbstractCacheRecordStore;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.Arrays;

/**
 * @author mdogan 11/02/14
 */
// TODO: needs refactor and cleanup..
public final class EnterpriseCacheHashMap extends BinaryOffHeapHashMap<EnterpriseCacheRecord> {

    private static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    // TODO: clear thread local at the end!
    private static final ThreadLocal<SortArea> SORT_AREA_THREAD_LOCAL = new ThreadLocal<SortArea>() {
        @Override
        protected SortArea initialValue() {
            return new SortArea();
        }
    };

    private final Callback<Data> evictionCallback;

    private int randomEvictionLastIndex;

    public EnterpriseCacheHashMap(int initialCapacity, EnterpriseSerializationService serializationService,
                                  MemoryBlockAccessor<EnterpriseCacheRecord> memoryBlockAccessor, Callback<Data> evictionCallback) {
        super(initialCapacity, serializationService, memoryBlockAccessor,
                serializationService.getMemoryManager().unwrapMemoryAllocator());
        this.evictionCallback = evictionCallback;
    }

    protected OffHeapOutOfMemoryError onOome(OffHeapOutOfMemoryError e) {
        return new OffHeapOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
    }

    public int evictExpiredRecords(int percentage) {
        int capacity = capacity();
        int len = (int) (capacity * (long) percentage / 100);
        int k = 0;
        if (len > 0 && size() > 0) {
            AbstractCacheRecordStore.CacheRecordAccessor service = getCacheRecordAccessor();
            int start = percentage < 100 ? (int) (Math.random() * capacity) : 0;
            int end = percentage < 100 ? Math.min(start + len, capacity) : capacity;

            long now = Clock.currentTimeMillis();
            for (int ix = start; ix < end; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    int ttlMillis = EnterpriseCacheRecord.getTtlMillis(value);
                    if (ttlMillis > 0) {
                        long creationTime = EnterpriseCacheRecord.getCreationTime(value);
                        if (creationTime + ttlMillis < now) {
                            long key = getKey(ix);
                            OffHeapData binary = service.readData(key);
                            callbackEvictionListeners(binary);
                            delete(binary);
                            service.disposeData(binary);
                            k++;
                        }
                    }
                }
            }
        }
        return k;
    }

    private void callbackEvictionListeners(OffHeapData binary) {
        if (evictionCallback != null) {
            evictionCallback.notify(binary);
        }
    }

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

    private int evictRecordsLRU(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= 100 || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }
        final int capacity = capacity();
        int k = 0;
        if (size > 0) {
            long[] sortArray = SORT_AREA_THREAD_LOCAL.get().getLongArray(size);
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
            int index = (int) (size * (long) percentage / 100);
            long time = sortArray[index];

            AbstractCacheRecordStore.CacheRecordAccessor service = getCacheRecordAccessor();
            k = 0;
            for (int ix = 0; ix < capacity && k < index; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    long accessTime = getAccessTime(value);
                    if (accessTime <= time) {
                        k++;
                        long key = getKey(ix);
                        OffHeapData keyData = service.readData(key);
                        delete(keyData);
                        service.disposeData(keyData);
                    }
                }
            }
        }
        return k;
    }

    private static long getAccessTime(long recordAddress) {
        long creationTime = EnterpriseCacheRecord.getCreationTime(recordAddress);
        int accessTimeDiff = EnterpriseCacheRecord.getAccessTimeDiff(recordAddress);
        return creationTime + accessTimeDiff;
    }

    private int evictRecordsLFU(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        final int size = size();
        if (percentage >= 100 || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }
        final int capacity = capacity();
        int k = 0;
        if (size > 0) {
            int hit;
            int[] sortArray = SORT_AREA_THREAD_LOCAL.get().getIntArray(size);
            for (int ix = 0; ix < capacity; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    hit = EnterpriseCacheRecord.getAccessHit(value);
                    sortArray[k] = hit;
                    if (++k >= size) {
                        break;
                    }
                }
            }
            Arrays.sort(sortArray, 0, size);
            int index = (int) (size * (long) percentage / 100);
            hit = sortArray[index];

            AbstractCacheRecordStore.CacheRecordAccessor service = getCacheRecordAccessor();
            k = 0;
            for (int ix = 0; ix < capacity && k < index; ix++) {
                if (isAllocated(ix)) {
                    long value = getValue(ix);
                    int h = EnterpriseCacheRecord.getAccessHit(value);
                    if (h <= hit) {
                        k++;
                        long key = getKey(ix);
                        OffHeapData keyData = service.readData(key);
                        delete(keyData);
                        service.disposeData(keyData);
                    }
                }
            }
        }
        return k;
    }

    private int evictRecordsRandom(int percentage) {
        if (percentage <= 0) {
            return 0;
        }
        int size = size();
        if (size == 0) {
            return 0;
        }

        if (percentage >= 100 || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        int len = (int) (size * (long) percentage / 100);
        len = Math.max(len, MIN_EVICTION_ELEMENT_COUNT);

        final int start = randomEvictionLastIndex;
        final int end = capacity();
        final int mask = end - 1;
        // assert capacity is power of 2, otherwise loop below will not work...
        // we know BinaryOffHeapHashMap.capacity() is power of 2
        assert QuickMath.isPowerOfTwo(end);

        int ix = start;
        int k = 0;
        AbstractCacheRecordStore.CacheRecordAccessor service = getCacheRecordAccessor();
        while (true) {
            if (isAllocated(ix)) {
                long key = getKey(ix);
                OffHeapData keyData = service.readData(key);
                delete(keyData);
                service.disposeData(keyData);
                if (++k >= len) {
                    break;
                }
            }
            ix = (ix + 1) & mask;
            if (ix == start) break;
        }
        randomEvictionLastIndex = ix;
        return k;
    }

    private AbstractCacheRecordStore.CacheRecordAccessor getCacheRecordAccessor() {
        return (AbstractCacheRecordStore.CacheRecordAccessor) memoryBlockAccessor;
    }

    public EntryIter iterator(int slot) {
        return new EntryIter(slot);
    }

    private static class SortArea {
        long[] longArray;
        int[] intArray;

        int[] getIntArray(int len) {
            if (intArray == null || intArray.length < len) {
                len = QuickMath.normalize(len, 50);
                intArray = new int[len];
            } else {
                Arrays.fill(intArray, 0, len, 0);
            }
            return intArray;
        }

        long[] getLongArray(int len) {
            if (longArray == null || longArray.length < len) {
                len = QuickMath.normalize(len, 50);
                longArray = new long[len];
            } else {
                Arrays.fill(longArray, 0, len, 0L);
            }
            return longArray;
        }
    }
}
