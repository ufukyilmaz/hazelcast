package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.hidensity.SampleableHiDensityCacheRecordMap;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author sozal 11/02/14
 */
public final class HiDensityNativeMemoryCacheRecordMap
        extends SampleableElasticHashMap<HiDensityNativeMemoryCacheRecord>
        implements SampleableHiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    private static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    private final transient HiDensityNativeMemoryCacheRecordProcessor cacheRecordProcessor;
    private final transient Callback<Data> evictionCallback;
    private final transient HiDensityCacheInfo cacheInfo;
    private int evictionLastIndex;
    private final transient Random random = new Random();

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
            HiDensityNativeMemoryCacheRecordProcessor cacheRecordProcessor,
            Callback<Data> evictionCallback,
            HiDensityCacheInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor);
        this.cacheRecordProcessor = cacheRecordProcessor;
        this.evictionCallback = evictionCallback;
        this.cacheInfo = cacheInfo;
    }

    @Override
    public boolean set(Data key, HiDensityNativeMemoryCacheRecord value) {
        boolean added = super.set(key, value);
        if (added) {
            cacheInfo.increaseEntryCount();
        }
        return added;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord put(Data key, HiDensityNativeMemoryCacheRecord value) {
        HiDensityNativeMemoryCacheRecord record = super.put(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            cacheInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord putIfAbsent(Data key, HiDensityNativeMemoryCacheRecord value) {
        HiDensityNativeMemoryCacheRecord record = super.putIfAbsent(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            cacheInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord replace(Data key, HiDensityNativeMemoryCacheRecord value) {
        return super.replace(key, value);
    }

    @Override
    public boolean delete(Data key) {
        boolean deleted = super.delete(key);
        if (deleted) {
            cacheInfo.decreaseEntryCount();
        }
        return deleted;
    }

    @Override
    public boolean remove(Object k, Object v) {
        boolean removed = super.remove(k, v);
        if (removed) {
            cacheInfo.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        cacheInfo.removeEntryCount(sizeBeforeClear);
    }

    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return new NativeOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
    }

    private void callbackEvictionListeners(NativeMemoryData binary) {
        if (evictionCallback != null) {
            evictionCallback.notify(binary);
        }
    }

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
                keys.add(memoryBlockProcessor.convertData(key, DataType.HEAP));
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

        if (evictionPercentage >= ICacheRecordStore.ONE_HUNDRED_PERCENT || size <= MIN_EVICTION_ELEMENT_COUNT) {
            clear();
            return size;
        }

        int len = (int) (size * (long) evictionPercentage / ICacheRecordStore.ONE_HUNDRED_PERCENT);
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
                NativeMemoryData keyData = cacheRecordProcessor.readData(key);
                delete(keyData);
                cacheRecordProcessor.disposeData(keyData);
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

    /**
     * Sampling entry that can be evictable and also it is an eviction candidate.
     */
    public class EvictableSamplingEntry extends SamplingEntry implements EvictionCandidate {

        public EvictableSamplingEntry(final int slot) {
            super(slot);
        }

        @Override
        public Object getAccessor() {
            return getKey();
        }

        @Override
        public Evictable getEvictable() {
            return (Evictable) getValue();
        }

    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(final int slot) {
        return new EvictableSamplingEntry(slot);
    }

    @Override
    public Iterable<EvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }

    @Override
    public <C extends EvictionCandidate<Data, HiDensityNativeMemoryCacheRecord>> int evict(
            Iterable<C> evictionCandidates) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, HiDensityNativeMemoryCacheRecord> evictionCandidate : evictionCandidates) {
            Data key = evictionCandidate.getAccessor();
            if (key instanceof NativeMemoryData) {
                boolean deleted = delete(key);
                cacheRecordProcessor.disposeData(key);
                if (deleted) {
                    actualEvictedCount++;
                }
            } else {
                if (remove(key) != null) {
                    actualEvictedCount++;
                }
            }
        }
        return actualEvictedCount;
    }

}
