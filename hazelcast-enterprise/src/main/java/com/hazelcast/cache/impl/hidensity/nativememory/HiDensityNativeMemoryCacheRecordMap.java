package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.cache.impl.CacheKeysWithCursor;
import com.hazelcast.cache.impl.hidensity.SampleableHiDensityCacheRecordMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.util.Clock;

import javax.cache.expiry.ExpiryPolicy;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;


public class HiDensityNativeMemoryCacheRecordMap
        extends SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryCacheRecord>
        implements SampleableHiDensityCacheRecordMap<HiDensityNativeMemoryCacheRecord> {

    private boolean entryCountingEnable;

    public HiDensityNativeMemoryCacheRecordMap(int initialCapacity,
                                               HiDensityRecordProcessor cacheRecordProcessor,
                                               HiDensityStorageInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor, cacheInfo);
    }

    // called by only the same partition thread. so there is no synchronization and visibility problem
    @Override
    public void setEntryCounting(boolean enable) {
        if (enable) {
            if (!entryCountingEnable) {
                // it was disable before but now it will be enable
                // therefore we increase the entry count as size of records
                storageInfo.addEntryCount(size());
            }
        } else {
            if (entryCountingEnable) {
                int size = size();
                // it was enable before but now it will be disable
                // therefore we decrease the entry count as size of records
                storageInfo.removeEntryCount(size);
            }
        }
        this.entryCountingEnable = enable;
    }

    @Override
    protected void increaseEntryCount() {
        if (entryCountingEnable) {
            super.increaseEntryCount();
        }
    }

    @Override
    protected void decreaseEntryCount() {
        if (entryCountingEnable) {
            super.decreaseEntryCount();
        }
    }

    @Override
    protected void decreaseEntryCount(int entryCount) {
        if (entryCountingEnable) {
            super.decreaseEntryCount(entryCount);
        }
    }

    @Override
    public CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        List<Data> keys = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchNext(pointers, size,
                (k, v) -> keys.add(memoryBlockProcessor.convertData(k, DataType.HEAP)));
        return new CacheKeysWithCursor(keys, newIterationPointers);
    }

    @Override
    public CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        List<Entry<Data, Data>> entries = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchNext(pointers, size, (k, v) -> {
            Data key = recordProcessor.convertData(k, DataType.HEAP);
            Data value = recordProcessor.convertData(v, DataType.HEAP);
            entries.add(new SimpleEntry<>(key, value));
        });
        return new CacheEntriesWithCursor(entries, newIterationPointers);
    }

    /**
     * Fetches at least {@code size} keys from the given {@code pointers} and
     * invokes the {@code entryConsumer} for each key-value pair.
     *
     * @param pointers         the pointers defining the state where to begin iteration
     * @param size             Count of how many entries will be fetched
     * @param keyValueConsumer the consumer to call with fetched key-value pairs
     * @return the pointers defining the state where iteration has ended
     */
    private IterationPointer[] fetchNext(IterationPointer[] pointers,
                                         int size,
                                         BiConsumer<Data, Data> keyValueConsumer) {
        long now = Clock.currentTimeMillis();
        IterationPointer[] updatedPointers = checkPointers(pointers, capacity());
        IterationPointer lastPointer = updatedPointers[updatedPointers.length - 1];
        int currentBaseSlot = lastPointer.getIndex();
        int[] fetchedEntries = {0};

        while (fetchedEntries[0] < size && currentBaseSlot >= 0) {
            currentBaseSlot = fetchAllWithBaseSlot(currentBaseSlot, slot -> {
                long valueAddr = accessor.getValue(slot);
                HiDensityNativeMemoryCacheRecord record = readV(valueAddr);
                if (!record.isExpiredAt(now)) {
                    long currentKey = accessor.getKey(slot);
                    int currentKeyHashCode = NativeMemoryDataUtil.hashCode(currentKey);
                    if (hasNotBeenObserved(currentKeyHashCode, updatedPointers)) {
                        NativeMemoryData keyData = accessor.keyData(slot);
                        keyValueConsumer.accept(keyData, record.getValue());
                        fetchedEntries[0]++;
                    }
                }
            });
            lastPointer.setIndex(currentBaseSlot);
        }
        // either fetched enough entries or there are no more entries to iterate over
        return updatedPointers;
    }

    /**
     * Checks the {@code pointers} to see if we need to restart iteration on the
     * current table and returns the updated pointers if necessary.
     *
     * @param pointers         the pointers defining the state of iteration
     * @param currentTableSize the current table size
     * @return the updated pointers, if necessary
     */
    private IterationPointer[] checkPointers(IterationPointer[] pointers, int currentTableSize) {
        IterationPointer lastPointer = pointers[pointers.length - 1];
        boolean iterationStarted = lastPointer.getSize() == -1;
        boolean tableResized = lastPointer.getSize() != currentTableSize;
        // clone pointers to avoid mutating given reference
        // add new pointer if resize happened during iteration
        int newLength = !iterationStarted && tableResized ? pointers.length + 1 : pointers.length;

        IterationPointer[] updatedPointers = new IterationPointer[newLength];
        for (int i = 0; i < pointers.length; i++) {
            updatedPointers[i] = new IterationPointer(pointers[i]);
        }

        // reset last pointer if we haven't started iteration or there was a resize
        if (iterationStarted || tableResized) {
            updatedPointers[updatedPointers.length - 1] = new IterationPointer(Integer.MAX_VALUE, currentTableSize);
        }
        return updatedPointers;
    }

    private final class CacheEvictableSamplingEntry
            extends EvictableSamplingEntry
            implements CacheEntryView {

        private CacheEvictableSamplingEntry(int slot) {
            super(slot);
        }

        @Override
        public long getExpirationTime() {
            return getEntryValue().getExpirationTime();
        }

        @Override
        public ExpiryPolicy getExpiryPolicy() {
            return (ExpiryPolicy) recordProcessor.toObject(getEntryValue().getExpiryPolicy());
        }
    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(final int slot) {
        return new CacheEvictableSamplingEntry(slot);
    }
}
