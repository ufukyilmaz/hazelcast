package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.NativeMapEntryCostEstimator;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Hi-Density backed {@code Storage} implementation for {@link com.hazelcast.core.IMap}.
 * This implementation can be used under multi-thread access.
 */
public class HDStorageImpl implements Storage<Data, HDRecord>, ForcedEvictable<HDRecord> {

    private final HiDensityRecordProcessor recordProcessor;
    private final EntryCostEstimator<NativeMemoryData, MemoryBlock> entryCostEstimator;
    private final HDStorageSCHM map;
    private volatile int entryCount;

    public HDStorageImpl(HiDensityRecordProcessor<HDRecord> recordProcessor, SerializationService serializationService) {
        this.recordProcessor = recordProcessor;
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
    public void updateRecordValue(Data ignored, HDRecord record, Object value) {
        Data oldValue = null;
        Data newValue = null;
        boolean succeed = false;
        long oldCostEstimate = 0L;

        try {
            oldValue = record.getValue();
            oldCostEstimate = entryCostEstimator.calculateValueCost(record);
            newValue = toNative(value);
            long address = value == null ? NULL_ADDRESS : ((NativeMemoryData) newValue).address();
            record.setValueAddress(address);
            succeed = true;
        } finally {
            if (succeed) {
                addDeferredDispose(oldValue);
                entryCostEstimator.adjustEstimateBy(-oldCostEstimate);
                entryCostEstimator.adjustEstimateBy(entryCostEstimator.calculateValueCost(record));
            } else {
                addDeferredDispose(newValue);
                entryCostEstimator.adjustEstimateBy(-entryCostEstimator.calculateValueCost((NativeMemoryData) newValue));
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
        map.clear();
        setEntryCount(0);
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
        disposeDeferredBlocks();
        map.dispose();
        setEntryCount(0);
        entryCostEstimator.reset();
    }

    public EntryCostEstimator getEntryCostEstimator() {
        return entryCostEstimator;
    }

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

    public Iterable getRandomSamples(int sampleCount) {
        return map.getRandomSamples(sampleCount);
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        return map.fetchKeys(tableIndex, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size, SerializationService serializationService) {
        return map.fetchEntries(tableIndex, size);
    }

    public long getNativeKeyAddress(Data key) {
        return map.getNativeKeyAddress(key);
    }

}
