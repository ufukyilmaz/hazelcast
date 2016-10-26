package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

/**
 * @author sozal 18/02/15
 *
 * @param <R> type of the {@link HiDensityRecord} to be stored
 */
public class EvictableHiDensityRecordMap<R extends HiDensityRecord & Evictable & Expirable>
        extends DefaultHiDensityRecordMap<R>
        implements EvictableStore<Data, R>, EvictionListener<Data, R> {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    // to reuse every time a key is read from a reference
    protected final NativeMemoryData keyHolder = new NativeMemoryData();

    public EvictableHiDensityRecordMap(int initialCapacity,
                                       HiDensityRecordProcessor<R> recordProcessor,
                                       HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
    }

    /**
     * Forcefully evict records with the given <code>evictionPercentage</code>.
     *
     * @param evictionPercentage    percentage to determine how many records will be evicted
     * @param evictionListener      {@link EvictionListener} to be notified
     *                              about evicted key and value
     * @return evicted entry count
     */
    public <C extends EvictionCandidate<Data, R>> int forceEvict(int evictionPercentage,
                                                                 EvictionListener<Data, R> evictionListener) {
        int size = size();
        if (evictionPercentage < 0 || size == 0) {
            return 0;
        }

        int evictCount = (int) (size * (long) evictionPercentage / ONE_HUNDRED_PERCENT);
        evictCount = Math.max(evictCount, MIN_EVICTION_ELEMENT_COUNT);


        SlottableIterator<Data> iter = newRandomEvictionKeyIterator();

        return forceEvict(iter, evictionListener, evictCount);
    }

    private int forceEvict(SlottableIterator<Data> iterator, EvictionListener<Data, R> evictionListener, int evictCount) {
        int evictedEntryCount = 0;
        while (iterator.hasNext()) {
            iterator.nextSlot();

            int slot = iterator.getCurrentSlot();
            keyHolder.reset(accessor.getKey(slot));
            R value = recordProcessor.read(accessor.getValue(slot));
            onEvict(keyHolder, value, false);

            if (evictionListener != null) {
                evictionListener.onEvict(keyHolder, value, false);
            }

            iterator.remove();
            if (++evictedEntryCount >= evictCount) {
                break;
            }
        }
        return evictedEntryCount;
    }

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

        // assert capacity is power of 2, otherwise loop below will not work
        // we know BinaryOffHeapHashMap.capacity() is power of 2
        assert QuickMath.isPowerOfTwo(end);

        int evictedEntryCount = 0;
        int ix = start;
        boolean shifted;

        while (true) {
            shifted = false;
            if (accessor.isAssigned(ix)) {
                long key = accessor.getKey(ix);
                NativeMemoryData keyData = recordProcessor.readData(key);
                R value = get(keyData);
                boolean evict;
                if (expirationChecker == null) {
                    evict = value.isExpiredAt(now);
                } else {
                    evict = expirationChecker.isExpired(value);
                }
                if (evict) {
                    onEvict(keyData, value, true);
                    if (evictionListener != null) {
                        evictionListener.onEvict(keyData, value, true);
                    }
                    delete(keyData);
                    recordProcessor.disposeData(keyData);
                    evictedEntryCount++;
                    // if disposed index is still allocated, it means next keys are shifted
                    shifted = accessor.isAssigned(ix);
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
            R removedRecord = remove(key);
            if (removedRecord != null) {
                actualEvictedCount++;
                onEvict(key, removedRecord, false);
                if (evictionListener != null) {
                    evictionListener.onEvict(key, removedRecord, false);
                }
                recordProcessor.dispose(removedRecord);
            }
            recordProcessor.disposeData(key);
        }
        return actualEvictedCount;
    }

    @Override
    public void onEvict(Data evictedEntryAccessor, R evictedEntry, boolean wasExpired) {
    }
}
