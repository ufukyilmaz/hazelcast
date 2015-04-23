package com.hazelcast.hidensity.impl;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictableStore;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.cache.impl.eviction.Expirable;
import com.hazelcast.cache.impl.eviction.ExpirationChecker;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be stored
 */
public class EvictableHiDensityRecordMap<R extends HiDensityRecord & Evictable & Expirable>
        extends DefaultHiDensityRecordMap<R>
        implements EvictableStore<Data, R> {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    protected final transient Callback<Data> evictionCallback;
    protected int evictionLastIndex;

    public EvictableHiDensityRecordMap(int initialCapacity,
                                       HiDensityRecordProcessor<R> recordProcessor,
                                       HiDensityStorageInfo storageInfo) {
        this(initialCapacity, recordProcessor, null, storageInfo);
    }

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

    /**
     * Forcefully evict records as given <code>evictionPercentage</code>.
     *
     * @param evictionPercentage Percentage to determine how many records will be evicted
     * @return evicted entry count
     */
    public int forceEvict(int evictionPercentage) {
        return doForceEvict(evictionPercentage, null);
    }

    /**
     * Forcefully evict records as given <code>evictionPercentage</code>.
     *
     * @param evictionPercentage    Percentage to determine how many records will be evicted
     * @param evictionListener      {@link com.hazelcast.cache.impl.eviction.EvictionListener} to be notified
     *                              about evicted key and value
     * @return evicted entry count
     */
    public <C extends EvictionCandidate<Data, R>> int forceEvict(int evictionPercentage,
                                                                 EvictionListener<Data, R> evictionListener) {
        return doForceEvict(evictionPercentage, evictionListener);
    }

    //CHECKSTYLE:OFF
    protected <C extends EvictionCandidate<Data, R>> int doForceEvict(int evictionPercentage,
                                                                      EvictionListener<Data, R> evictionListener) {
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
                R value = remove(keyData);
                if (evictionListener != null) {
                    evictionListener.onEvict(keyData, value);
                }
                if (value != null) {
                    memoryBlockProcessor.dispose(value);
                }
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

    public int evictExpiredRecords() {
        return doEvictExpiredRecords(null, null);
    }

    public int evictExpiredRecords(EvictionListener<Data, R> evictionListener) {
        return doEvictExpiredRecords(evictionListener, null);
    }

    public int evictExpiredRecords(ExpirationChecker<R> expirationChecker) {
        return doEvictExpiredRecords(null, expirationChecker);
    }

    public int evictExpiredRecords(EvictionListener<Data, R> evictionListener,
                                   ExpirationChecker<R> expirationChecker) {
        return doEvictExpiredRecords(evictionListener, expirationChecker);
    }

    //CHECKSTYLE:OFF
    protected <C extends EvictionCandidate<Data, R>> int doEvictExpiredRecords(
            EvictionListener<Data, R> evictionListener, ExpirationChecker<R> expirationChecker) {
        final long now = Clock.currentTimeMillis();
        final int start = 0;
        final int end = capacity();
        final int mask = end - 1;

        // assert capacity is power of 2, otherwise loop below will not work...
        // we know BinaryOffHeapHashMap.capacity() is power of 2
        assert QuickMath.isPowerOfTwo(end);

        int evictedEntryCount = 0;
        int ix = start;
        boolean shifted;

        while (true) {
            shifted = false;
            if (isAllocated(ix)) {
                long key = getKey(ix);
                NativeMemoryData keyData = recordProcessor.readData(key);
                R value = get(keyData);
                boolean evict;
                if (expirationChecker == null) {
                    evict = value.isExpiredAt(now);
                } else {
                    evict = expirationChecker.isExpired(value);
                }
                if (evict) {
                    if (evictionListener != null) {
                        evictionListener.onEvict(keyData, value);
                    }
                    delete(keyData);
                    recordProcessor.disposeData(keyData);
                    evictedEntryCount++;
                    // If disposed key is still allocated, this means next keys are shifted
                    shifted = isAllocated(ix);
                }
            }
            if (!shifted) {
                ix = (ix + 1) & mask;
                if (ix == start) {
                    break;
                }
            }
        }
        return evictedEntryCount;
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
                        evictionListener.onEvict(evictionCandidate.getAccessor(), evictionCandidate.getEvictable());
                    }
                }
            }
        }
        return actualEvictedCount;
    }

}
