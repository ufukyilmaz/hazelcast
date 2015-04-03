package com.hazelcast.hidensity.impl;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictableStore;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.QuickMath;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be stored
 */
public class EvictableHiDensityRecordMap<R extends HiDensityRecord & Evictable>
        extends DefaultHiDensityRecordMap<R>
        implements EvictableStore<Data, R> {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    protected final transient Callback<Data> evictionCallback;
    protected int evictionLastIndex;

    public EvictableHiDensityRecordMap(int initialCapacity,
                                       HiDensityRecordProcessor<R> recordProcessor,
                                       Callback<Data> evictionCallback,
                                       HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
        this.evictionCallback = evictionCallback;
    }

    protected void callbackEvictionListeners(NativeMemoryData data) {
        if (evictionCallback != null) {
            evictionCallback.notify(data);
        }
    }

    //CHECKSTYLE:OFF
    /**
     * Forcefully evict records as given <code>evictionPercentage</code>.
     *
     * @param evictionPercentage Percentage to determine how many records will be evicted
     * @return evicted entry count
     */
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

    @Override
    public <C extends EvictionCandidate<Data, R>> int evict(
            Iterable<C> evictionCandidates,
            EvictionListener<Data, R> evictionListener) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, R> evictionCandidate : evictionCandidates) {
            Data key = evictionCandidate.getAccessor();
            if (key instanceof NativeMemoryData) {
                boolean deleted = delete(key);
                recordProcessor.disposeData(key);
                if (deleted) {
                    actualEvictedCount++;
                }
            } else {
                if (remove(key) != null) {
                    actualEvictedCount++;
                    if (evictionListener != null) {
                        evictionListener.onEvict(evictionCandidate);
                    }
                }
            }
        }
        return actualEvictedCount;
    }

}
