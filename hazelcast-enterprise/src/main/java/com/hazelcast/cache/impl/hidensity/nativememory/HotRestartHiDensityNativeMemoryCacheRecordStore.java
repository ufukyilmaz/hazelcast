package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreHelper;
import com.hazelcast.internal.hotrestart.RecordDataSink;
import com.hazelcast.internal.hotrestart.impl.KeyOffHeap;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.internal.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Iterator;
import java.util.UUID;

import static com.hazelcast.internal.serialization.DataType.NATIVE;

/**
 * NativeMemory cache record store with Hot Restart support.
 */
public class HotRestartHiDensityNativeMemoryCacheRecordStore
        extends HiDensityNativeMemoryCacheRecordStore
        implements RamStore {

    private static final boolean ASSERTION_ENABLED;

    static {
        ASSERTION_ENABLED = HotRestartHiDensityNativeMemoryCacheRecordStore.class.desiredAssertionStatus();
    }

    private final long prefix;
    private final boolean fsync;
    private final HotRestartStore hotRestartStore;

    /**
     * See {@link HotRestartHiDensityNativeMemoryCacheRecordMap#mutex}
     */
    private final Object mutex;

    public HotRestartHiDensityNativeMemoryCacheRecordStore(
            int partitionId, String cacheNameWithPrefix, EnterpriseCacheService cacheService,
            NodeEngine nodeEngine, boolean fsync, long keyPrefix) {
        super(partitionId, cacheNameWithPrefix, cacheService, nodeEngine);
        this.fsync = fsync;
        this.prefix = keyPrefix;
        this.hotRestartStore = cacheService.offHeapHotRestartStoreForPartition(partitionId);
        assert hotRestartStore != null;

        HotRestartHiDensityNativeMemoryCacheRecordMap recordMap =
                (HotRestartHiDensityNativeMemoryCacheRecordMap) records;
        mutex = recordMap.getMutex();
        initMap(recordMap);
    }

    private void initMap(HotRestartHiDensityNativeMemoryCacheRecordMap recordMap) {
        recordMap.setPrefix(prefix);
        recordMap.setHotRestartStore(hotRestartStore);
        recordMap.setFsync(fsync);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecordMap createMapInternal(int capacity) {
        return new HotRestartHiDensityNativeMemoryCacheRecordMap(capacity, cacheRecordProcessor, cacheInfo);
    }

    @Override
    long newSequence() {
        return memoryManager.newSequence();
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord doPutRecord(Data key, HiDensityNativeMemoryCacheRecord record,
                                                           UUID source, boolean updateJournal) {
        HiDensityNativeMemoryCacheRecord oldRecord = super.doPutRecord(key, record, source, updateJournal);
        putToHotRestart(key, record);
        return oldRecord;
    }

    @Override
    protected void updateRecordValue(HiDensityNativeMemoryCacheRecord record, Object recordValue) {
        synchronized (mutex) {
            record.setValue((NativeMemoryData) recordValue);
        }
    }

    @Override
    protected void onUpdateRecord(Data key, HiDensityNativeMemoryCacheRecord record, Object value, Data oldDataValue) {
        super.onUpdateRecord(key, record, value, oldDataValue);
        putToHotRestart(key, record);
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        lookupAndRemoveFromHotRestart(key);
        return super.removeRecord(key);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord doRemoveRecord(Data key, UUID source) {
        lookupAndRemoveFromHotRestart(key);
        return super.doRemoveRecord(key, source);
    }

    private void lookupAndRemoveFromHotRestart(Data key) {
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (isMemoryBlockValid(record)) {
            removeFromHotRestart(key, record);
        }
    }

    @Override
    protected void onOwn(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                         NativeMemoryData oldValueData, boolean isNewPut, boolean disableDeferredDispose) {
        putToHotRestart(key, record);
        super.onOwn(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose);
    }

    @Override
    public void disposeDeferredBlocks() {
        synchronized (mutex) {
            super.disposeDeferredBlocks();
        }
    }

    private void putToHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        NativeMemoryData value = record.getValue();
        assert value != null : "Value should not be null! -> " + record;
        byte[] valueBytes = value.toByteArray();
        hotRestartStore.put(newHotRestartKey(key, record), valueBytes, fsync);
    }

    private void removeFromHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        final KeyOffHeap hotRestartKey = newHotRestartKey(key, record);
        hotRestartStore.remove(hotRestartKey, fsync);
    }

    private KeyOffHeap newHotRestartKey(Data key, HiDensityNativeMemoryCacheRecord record) {
        long keyAddress = records.getNativeKeyAddress(key);
        assert keyAddress != NULL_PTR : "Invalid key address!";
        assert record.address() != NULL_PTR;
        assert record.getValueAddress() != NULL_PTR;
        return new KeyOffHeap(prefix, key.toByteArray(), keyAddress, record.getSequence());
    }

    // called from Hot Restart GC thread
    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) {
        KeyHandleOffHeap kh = (KeyHandleOffHeap) keyHandle;
        assert kh.address() != NULL_PTR;

        synchronized (mutex) {
            NativeMemoryData key = RamStoreHelper.validateAndGetKey(kh, memoryManager);
            if (key == null) {
                return false;
            }
            HiDensityNativeMemoryCacheRecord record = records.getIfSameKey(key);
            return record != null && RamStoreHelper.copyEntry(kh, key, record, expectedSize, sink);
        }
    }

    // called from PartitionOperationThread
    @Override
    public KeyHandleOffHeap toKeyHandle(byte[] keyBytes) {
        assert keyBytes != null && keyBytes.length > 0;
        final HeapData heapKey = new HeapData(keyBytes);

        final long nativeKeyAddress = records.getNativeKeyAddress(heapKey);
        if (nativeKeyAddress != NULL_PTR) {
            return readExistingKeyHandle(nativeKeyAddress);
        }
        return newKeyHandle(heapKey);
    }

    // called from PartitionOperationThread
    @Override
    public void accept(KeyHandle kh, byte[] valueBytes) {
        assert kh != null : "accept() called with null KeyHandle";
        assert valueBytes != null && valueBytes.length > 0 : "accept() called with null/empty value";

        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final HiDensityNativeMemoryCacheRecord record = records.get(new NativeMemoryData().reset(ohk.address()));
        assert record != null : "accept() caled with unknown key";
        assert ohk.sequenceId() == record.getSequence() : String.format(
                "Sequence ID of the supplied keyHandle (%d) doesn't match the one in RamStore (%d)",
                ohk.sequenceId(), record.getSequence());
        acceptNewValue(record, new HeapData(valueBytes));
    }

    // called from PartitionOperationThread
    @Override
    public void removeNullEntries(SetOfKeyHandle keyHandles) {
        SetOfKeyHandle.KhCursor cursor = keyHandles.cursor();
        NativeMemoryData key = new NativeMemoryData();
        while (cursor.advance()) {
            KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) cursor.asKeyHandle();
            key.reset(keyHandle.address());
            HiDensityNativeMemoryCacheRecord record = records.remove(key);
            assert record != null;
            assert record.getValueAddress() == NULL_PTR;
            assert record.getSequence() == keyHandle.sequenceId();
            cacheRecordProcessor.dispose(record);
            cacheRecordProcessor.disposeData(key);
        }

        // DEBUG
        if (ASSERTION_ENABLED) {
            scanEmptyRecords();
        }
    }

    @Override
    public void reset() {
        resetInternal(true);
    }

    private void resetInternal(boolean clearHotRestartStore) {
        if (clearHotRestartStore) {
            hotRestartStore.clear(fsync, prefix);
        }
        super.reset();
    }

    @Override
    public void close(boolean onShutdown) {
        if (shouldExplicitlyClear(onShutdown)) {
            resetInternal(false);
        }
        destroyEventJournal();
        records.dispose();
        closeListeners();
    }

    private KeyHandleOffHeap readExistingKeyHandle(long nativeKeyAddress) {
        NativeMemoryData key = new NativeMemoryData().reset(nativeKeyAddress);
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        assert record != null;
        return new SimpleHandleOffHeap(key.address(), record.getSequence());
    }

    private KeyHandleOffHeap newKeyHandle(HeapData heapKey) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        NativeMemoryData nativeKey = (NativeMemoryData) recordProcessor.convertData(heapKey, NATIVE);
        long recordSequence = newSequence();
        acceptNewRecord(nativeKey, recordSequence);
        return new SimpleHandleOffHeap(nativeKey.address(), recordSequence);
    }

    private void acceptNewRecord(NativeMemoryData key, long recordSequence) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        HiDensityNativeMemoryCacheRecord record = null;
        try {
            record = createRecordInternal(null, Clock.currentTimeMillis(), Long.MAX_VALUE, recordSequence);
            boolean isNewRecord = records.set(key, record);
            assert isNewRecord;
        } catch (NativeOutOfMemoryError e) {
            recordProcessor.disposeData(key);
            if (record != null) {
                recordProcessor.dispose(record);
            }
            throw e;
        }
    }

    private void acceptNewValue(HiDensityNativeMemoryCacheRecord record, HeapData value) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        NativeMemoryData nativeValue = (NativeMemoryData) recordProcessor.convertData(value, NATIVE);
        recordProcessor.disposeValue(record);
        record.setValue(nativeValue);
    }

    private void scanEmptyRecords() {
        Iterator<HiDensityNativeMemoryCacheRecord> iter = records.valueIter();
        while (iter.hasNext()) {
            HiDensityNativeMemoryCacheRecord record = iter.next();
            assert record != null;
            assert record.getValueAddress() != NULL_PTR;
        }
    }
}
