package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.NativeMapEntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Hi-Density backed {@code Storage} implementation for {@link IMap}.
 * This implementation can be used under multi-thread access.
 */
public class HDStorageImpl implements Storage<Data, HDRecord>, ForcedEvictable<HDRecord> {

    private final HDStorageSCHM map;
    private final HiDensityStorageInfo storageInfo;
    private final HiDensityRecordProcessor recordProcessor;
    private final EntryCostEstimator<NativeMemoryData, MemoryBlock> entryCostEstimator;

    private volatile int entryCount;

    public HDStorageImpl(HiDensityRecordProcessor<HDRecord> recordProcessor, SerializationService serializationService) {
        this.recordProcessor = recordProcessor;
        this.storageInfo = ((DefaultHiDensityRecordProcessor) recordProcessor).getStorageInfo();
        this.entryCostEstimator = new NativeMapEntryCostEstimator(recordProcessor);
        this.map = new HDStorageSCHM(recordProcessor, serializationService);
    }

    public HiDensityRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    @Override
    public void removeRecord(HDRecord record) {
        if (record == null) {
            return;
        }

        Data key = record.getKey();
        HDRecord oldRecord = map.remove(key);

        addDeferredDispose(key);
        addDeferredDispose(oldRecord);

        entryCostEstimator.adjustEstimateBy(-entryCostEstimator.calculateEntryCost((NativeMemoryData) key, record));
        storageInfo.decreaseEntryCount();
        setEntryCount(map.size());
    }

    @Override
    public boolean containsKey(Data key) {
        return map.containsKey(key);
    }

    @Override
    public void put(Data key, HDRecord record) {
        HDRecord oldRecord = null;
        NativeMemoryData nativeKey = null;
        boolean succeed = false;
        try {
            nativeKey = toNative(key);
            record.setKeyAddress(nativeKey.address());
            oldRecord = map.put(nativeKey, record);
            succeed = true;
        } finally {
            if (succeed) {
                addDeferredDispose(oldRecord);
                if (oldRecord != null) {
                    entryCostEstimator.adjustEstimateBy(-entryCostEstimator.calculateValueCost(oldRecord));
                    entryCostEstimator.adjustEstimateBy(entryCostEstimator.calculateValueCost(record));
                } else {
                    entryCostEstimator.adjustEstimateBy(entryCostEstimator.calculateEntryCost(nativeKey, record));
                    storageInfo.increaseEntryCount();
                }
            } else {
                addDeferredDispose(record);
                addDeferredDispose(nativeKey);
                entryCostEstimator.adjustEstimateBy(-entryCostEstimator.calculateEntryCost(nativeKey, record));
            }
        }

        setEntryCount(map.size());
    }

    @Override
    public void updateRecordValue(Data key, HDRecord record, Object value) {
        NativeMemoryData oldValue = null;
        NativeMemoryData newValue = null;
        boolean disposeNewValue = true;
        long oldCostEstimate = 0L;

        try {
            oldValue = (NativeMemoryData) record.getValue();
            oldCostEstimate = entryCostEstimator.calculateValueCost(record);
            newValue = (NativeMemoryData) toNative(value);

            if (oldValue != null && newValue != null && oldValue.address() == newValue.address()) {
                disposeNewValue = false;
                return;
            }

            long address = value == null ? NULL_ADDRESS : newValue.address();
            record.setValueAddress(address);
            disposeNewValue = false;
            addDeferredDispose(oldValue);
            entryCostEstimator.adjustEstimateBy(-oldCostEstimate);
            entryCostEstimator.adjustEstimateBy(entryCostEstimator.calculateValueCost(record));
        } finally {
            if (disposeNewValue) {
                addDeferredDispose(newValue);
                entryCostEstimator.adjustEstimateBy(-entryCostEstimator.calculateValueCost(newValue));
            }
        }
    }

    @Override
    public HDRecord get(Data key) {
        return map.get(key);
    }

    @Override
    public HDRecord getIfSameKey(Data key) {
        return map.getIfSameKey(key);
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        HazelcastMemoryManager memoryManager = ((DefaultHiDensityRecordProcessor) recordProcessor).getMemoryManager();
        if (memoryManager == null || memoryManager.isDisposed()) {
            // otherwise will cause a SIGSEGV
            return;
        }

        long entryCount = map.size();
        map.clear();
        setEntryCount(0);
        storageInfo.removeEntryCount(entryCount);
        entryCostEstimator.reset();
    }

    private void setEntryCount(int value) {
        entryCount = value;
    }

    @Override
    public Collection<HDRecord> values() {
        return map.values();
    }

    @Override
    public Iterator<HDRecord> mutationTolerantIterator() {
        return map.valueIter(false);
    }

    @Override
    public Iterator<HDRecord> newForcedEvictionValuesIterator() {
        return map.newRandomEvictionValueIterator();
    }

    @Override
    public int size() {
        return entryCount;
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void destroy(boolean isDuringShutdown) {
        HazelcastMemoryManager memoryManager = ((DefaultHiDensityRecordProcessor) recordProcessor).getMemoryManager();
        if (memoryManager == null || memoryManager.isDisposed()) {
            // otherwise will cause a SIGSEGV
            return;
        }

        long entryCount = map.size();
        disposeDeferredBlocks();
        map.dispose();
        setEntryCount(0);
        storageInfo.removeEntryCount(entryCount);
        entryCostEstimator.reset();
    }

    @Override
    public EntryCostEstimator getEntryCostEstimator() {
        return entryCostEstimator;
    }

    @Override
    public void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disposeDeferredBlocks() {
        recordProcessor.disposeDeferredBlocks();
    }

    protected void addDeferredDispose(Object memoryBlock) {
        if (memoryBlock == null
                || ((MemoryBlock) memoryBlock).address() == NULL_ADDRESS
                || memoryBlock instanceof HeapData
                || !(memoryBlock instanceof MemoryBlock)) {
            return;
        }

        recordProcessor.addDeferredDispose(((MemoryBlock) memoryBlock));
    }

    protected NativeMemoryData toNative(Data key) {
        return (NativeMemoryData) recordProcessor.convertData(key, DataType.NATIVE);
    }

    private Data toNative(Object value) {
        return recordProcessor.toData(value, DataType.NATIVE);
    }

    @Override
    public Iterable getRandomSamples(int sampleCount) {
        return map.getRandomSamples(sampleCount);
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        return map.fetchKeys(tableIndex, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size,
                                             SerializationService serializationService) {
        return map.fetchEntries(tableIndex, size);
    }

    @Override
    public Record extractRecordFromLazy(EntryView entryView) {
        return ((HDStorageSCHM.LazyEntryViewFromRecord) entryView).getRecord();
    }

    public long getNativeKeyAddress(Data key) {
        return map.getNativeKeyAddress(key);
    }

}