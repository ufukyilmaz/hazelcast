/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.cache.HiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
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
        extends BinaryOffHeapHashMap<HiDensityNativeMemoryCacheRecord>
        implements HiDensityCacheRecordMap<Data, HiDensityNativeMemoryCacheRecord> {

    private static final int MIN_EVICTION_ELEMENT_COUNT = 10;
    private static final int NORMALIZE_FACTORY = 50;

    // TODO clear thread local at the end!
    private static final ThreadLocal<SortArea> SORT_AREA_THREAD_LOCAL = new ThreadLocal<SortArea>() {
        @Override
        protected SortArea initialValue() {
            return new SortArea();
        }
    };

    private final transient HiDensityNativeMemoryCacheRecordStore.HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor;
    private final transient Callback<Data> evictionCallback;
    private int randomEvictionLastIndex;
    private final Random random = new Random();

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
            EnterpriseSerializationService serializationService,
            HiDensityNativeMemoryCacheRecordStore.HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor,
            Callback<Data> evictionCallback) {
        super(initialCapacity, serializationService, cacheRecordAccessor,
              serializationService.getMemoryManager().unwrapMemoryAllocator());
        this.cacheRecordAccessor = cacheRecordAccessor;
        this.evictionCallback = evictionCallback;
    }

    protected OffHeapOutOfMemoryError onOome(OffHeapOutOfMemoryError e) {
        return new OffHeapOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
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
                            OffHeapData binary = cacheRecordAccessor.readData(key);
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

    private void callbackEvictionListeners(OffHeapData binary) {
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
                        OffHeapData keyData = cacheRecordAccessor.readData(key);
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
            int[] sortArray = SORT_AREA_THREAD_LOCAL.get().getIntArray(size);
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
                        OffHeapData keyData = cacheRecordAccessor.readData(key);
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
                OffHeapData keyData = cacheRecordAccessor.readData(key);
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
        BinaryOffHeapHashMap<HiDensityNativeMemoryCacheRecord>.EntryIter iter =
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

    /**
     * Sort area for record address as thread local usage.
     */
    private static class SortArea {
        long[] longArray;
        int[] intArray;

        int[] getIntArray(int len) {
            if (intArray == null || intArray.length < len) {
                len = QuickMath.normalize(len, NORMALIZE_FACTORY);
                intArray = new int[len];
            } else {
                Arrays.fill(intArray, 0, len, 0);
            }
            return intArray;
        }

        long[] getLongArray(int len) {
            if (longArray == null || longArray.length < len) {
                len = QuickMath.normalize(len, NORMALIZE_FACTORY);
                longArray = new long[len];
            } else {
                Arrays.fill(longArray, 0, len, 0L);
            }
            return longArray;
        }
    }

}
