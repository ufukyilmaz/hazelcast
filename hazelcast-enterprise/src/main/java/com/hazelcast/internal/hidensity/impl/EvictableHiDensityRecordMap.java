package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.util.Clock;

import java.util.Iterator;
import java.util.Map;

/**
 * @param <R> type of the {@link HiDensityRecord} to be stored
 */
public class EvictableHiDensityRecordMap<R extends HiDensityRecord & Evictable & Expirable>
        extends DefaultHiDensityRecordMap<R>
        implements EvictableStore<Data, R>, EvictionListener<Data, R> {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int MIN_EVICTION_ELEMENT_COUNT = 10;

    protected Iterator<Map.Entry<Data, R>> expirationIterator;

    // to reuse every time a key is read from a reference
    protected final NativeMemoryData keyHolder = new NativeMemoryData();

    public EvictableHiDensityRecordMap(int initialCapacity,
                                       HiDensityRecordProcessor<R> recordProcessor,
                                       HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor, storageInfo);
    }

    /**
     * Forcefully evict records with the given {@code evictionPercentage}.
     *
     * @param evictionPercentage percentage to determine how many records will be evicted
     * @param evictionListener   {@link EvictionListener} to be notified about evicted key and value
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

    /**
     * Scans entries to delete expired ones.
     *
     * This method is not thread safe and is called under lock.
     *
     * A background task calls this method periodically. We expect from this
     * method to find and delete expired entries eventually. For this purpose,
     * in each call, it only scans a fixed number of entries. {@link
     * #expirationIterator} is used for this scanning.
     *
     * @param evictionListener  used to listen evicted entries
     * @param expirationChecker used to check entries for expiry
     * @param maxScannableNum   scan at most this number of entries
     * @see #initExpirationIterator()
     */
    public void scanByNumberToDeleteExpired(EvictionListener<Data, R> evictionListener,
                                            ExpirationChecker<R> expirationChecker, int maxScannableNum) {

        long now = Clock.currentTimeMillis();
        int scannedNum = 0;
        initExpirationIterator();

        while (expirationIterator.hasNext()) {
            if (scannedNum >= maxScannableNum) {
                break;
            }
            scannedNum++;
            Entry<Data, R> entry = expirationIterator.next();
            R record = entry.getValue();
            if (record == null) {
                continue;
            }

            boolean expired = expirationChecker == null
                    ? record.isExpiredAt(now) : expirationChecker.isExpired(record);

            if (expired) {
                Data keyData = entry.getKey();
                onEvict(keyData, record, true);
                if (evictionListener != null) {
                    evictionListener.onEvict(keyData, record, true);
                }
                delete(keyData);
                recordProcessor.disposeData(keyData);
            }
        }
    }

    /**
     * Initializes {@link #expirationIterator} when it is either null or
     * reached to the end of iteration.
     */
    private void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = entryIter(false);
        }
    }

    @Override
    public <C extends EvictionCandidate<Data, R>> boolean tryEvict(C
                                                                           evictionCandidate,
                                                                   EvictionListener<Data, R> evictionListener) {
        if (evictionCandidate == null) {
            return false;
        }
        boolean evicted = false;
        Data key = evictionCandidate.getAccessor();
        R removedRecord = remove(key);
        if (removedRecord != null) {
            evicted = true;
            onEvict(key, removedRecord, false);
            if (evictionListener != null) {
                evictionListener.onEvict(key, removedRecord, false);
            }
            recordProcessor.dispose(removedRecord);
        }
        recordProcessor.disposeData(key);
        return evicted;
    }

    @Override
    public void onEvict(Data evictedEntryAccessor, R evictedEntry,
                        boolean wasExpired) {
    }
}
