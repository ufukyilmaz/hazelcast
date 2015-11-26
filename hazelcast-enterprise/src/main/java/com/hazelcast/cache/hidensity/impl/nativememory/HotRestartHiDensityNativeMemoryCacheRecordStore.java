package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;

import java.util.Collection;

import static com.hazelcast.nio.serialization.DataType.NATIVE;

/**
 * NativeMemory cache record store with Hot Restart support.
 */
public class HotRestartHiDensityNativeMemoryCacheRecordStore
        extends HiDensityNativeMemoryCacheRecordStore
        implements RamStore {

    private final long prefix;
    private final HotRestartStore hotRestartStore;

    /**
     * See {@link HotRestartHiDensityNativeMemoryCacheRecordMap#mutex}
     */
    private final Object recordMapMutex;

    private int tombstoneCount;

    private HiDensityNativeMemoryCacheRecord fetchedRecordDuringRestart;

    public HotRestartHiDensityNativeMemoryCacheRecordStore(
            int partitionId, String name, EnterpriseCacheService cacheService, NodeEngine nodeEngine, long keyPrefix) {
        super(partitionId, name, cacheService, nodeEngine);
        this.prefix = keyPrefix;
        this.hotRestartStore = cacheService.offHeapHotRestartStoreForCurrentThread();
        recordMapMutex = ((HotRestartHiDensityNativeMemoryCacheRecordMap) records).getMutex();
    }

    @Override
    protected HiDensityNativeMemoryCacheRecordMap createMapInternal(int capacity) {
        return new HotRestartHiDensityNativeMemoryCacheRecordMap(capacity, cacheRecordProcessor, cacheInfo);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord doPutRecord(Data key, HiDensityNativeMemoryCacheRecord record, String source) {
        HiDensityNativeMemoryCacheRecord oldRecord = super.doPutRecord(key, record, source);
        putToHotRestart(key, record);
        return oldRecord;
    }

    @Override
    protected void updateRecordValue(HiDensityNativeMemoryCacheRecord record, Object recordValue) {
        synchronized (recordMapMutex) {
            record.setValue((NativeMemoryData) recordValue);
        }
    }

    @Override
    protected void onUpdateRecord(Data key, HiDensityNativeMemoryCacheRecord record, Object value, Data oldDataValue) {
        super.onUpdateRecord(key, record, value, oldDataValue);
        putToHotRestart(key, record);
        if (oldDataValue == null) {
            record.setTombstoneSequence(0);
            tombstoneCount--;
            cacheInfo.increaseEntryCount();
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        CacheRecord recordToReturn = null;
        // If removed record is valid, first get a heap based copy of it and dispose it
        if (record != null && !record.isTombstone()) {
            recordToReturn = toHeapCacheRecord(record);
            synchronized (recordMapMutex) {
                cacheRecordProcessor.disposeValue(record);
            }
            removeFromHotRestart(key, record);
        }
        return recordToReturn;
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord doRemoveRecord(Data key, String source) {
        // Don't remove the record! We'll use it later as tombstone
        // see #onDeleteRecord() and #onRemove().
        HiDensityNativeMemoryCacheRecord removedRecord = records.get(key);
        boolean removed = removedRecord != null && removedRecord.getValueAddress() != NULL_PTR;
        if (removed) {
            invalidateEntry(key, source);
        }
        return removed ? removedRecord : null;
    }

    @Override
    protected void onDeleteRecord(Data key, HiDensityNativeMemoryCacheRecord record,
            Data dataValue, boolean deleted) {
        // If record is deleted and if this record is valid, dispose its data
        // Keeping record itself because its needed as a tombstone for hotrestart later.
        // Hotrestart will call #releaseTombstones() to actually remove the record
        if (deleted && isMemoryBlockValid(record)) {
            synchronized (recordMapMutex) {
                cacheRecordProcessor.disposeValue(record);
            }
        }
    }

    @Override
    protected void onRemove(Data key, Object value, String caller, boolean getValue,
                            HiDensityNativeMemoryCacheRecord record, boolean removed) {
        if (removed) {
            removeFromHotRestart(key, record);
        }
        onEntryInvalidated(key, caller);
    }

    @Override
    public void onEvict(Data key, HiDensityNativeMemoryCacheRecord record) {
        super.onEvict(key, record);
        assert records.containsKey(key);
        removeFromHotRestart(key, record);
    }

    @Override
    protected void onProcessExpiredEntry(Data key, HiDensityNativeMemoryCacheRecord record, long expiryTime, long now,
            String source, String origin) {

        if (isMemoryBlockValid(record)) {
            synchronized (recordMapMutex) {
                cacheRecordProcessor.disposeValue(record);
            }

            removeFromHotRestart(key, record);
        }
    }

    @Override
    protected void onOwn(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
            NativeMemoryData oldValueData, boolean isNewPut, boolean disableDeferredDispose) {

        putToHotRestart(key, record);
        if (!isNewPut) {
            tombstoneCount--;
            cacheInfo.increaseEntryCount();
        }
    }

    private void putToHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        NativeMemoryData value = record.getValue();
        assert value != null : "Value should not be null! -> " + record;
        byte[] valueBytes = value.toByteArray();
        hotRestartStore.put(newHotRestartKey(key, record), valueBytes);
    }

    private void removeFromHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        final KeyOffHeap hotRestartKey = newHotRestartKey(key, record);
        final long tombstoneSeq = hotRestartStore.removeStep1(hotRestartKey);
        synchronized (recordMapMutex) {
            record.setTombstoneSequence(tombstoneSeq);
        }
        hotRestartStore.removeStep2();
        tombstoneCount++;
        cacheInfo.decreaseEntryCount();
    }

    private KeyOffHeap newHotRestartKey(Data key, HiDensityNativeMemoryCacheRecord record) {
        long keyAddress = records.getNativeKeyAddress(key);
        assert keyAddress != NULL_PTR : "Invalid key address!";
        return new KeyOffHeap(prefix, key.toByteArray(), keyAddress, record.getSequence());
    }

    // called from hotrestart GC thread
    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) {
        KeyHandleOffHeap kh = (KeyHandleOffHeap) keyHandle;
        assert kh.address() != NULL_PTR;

        synchronized (recordMapMutex) {
            NativeMemoryData key = new NativeMemoryData().reset(kh.address());
            HiDensityNativeMemoryCacheRecord record = records.get(key);
            if (record == null) {
                return false;
            }
            return RamStoreHelper.copyEntry(kh, key, record, expectedSize, sink);
        }
    }

    // called from hotrestart GC thread
    @Override
    public void releaseTombstones(final Collection<TombstoneId> keysToRelease) {
        InternalOperationService opService = (InternalOperationService) nodeEngine.getOperationService();
        opService.execute(new TombstoneCleanerTask(keysToRelease));
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

    private KeyHandleOffHeap newKeyHandle(HeapData heapKey) {
        fetchedRecordDuringRestart = null;
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        NativeMemoryData nativeKey = (NativeMemoryData) recordProcessor.convertData(heapKey, NATIVE);
        long recordSequence = incrementSequence();
        return new SimpleHandleOffHeap(nativeKey.address(), recordSequence);
    }

    private KeyHandleOffHeap readExistingKeyHandle(long nativeKeyAddress) {
        NativeMemoryData key = new NativeMemoryData().reset(nativeKeyAddress);
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        assert record != null;
        // fetchedRecordDuringRestart will be used in #accept() method
        fetchedRecordDuringRestart = record;
        return new SimpleHandleOffHeap(key.address(), record.getSequence());
    }

    // called from PartitionOperationThread
    @Override
    public void accept(KeyHandle kh, byte[] valueBytes) {
        assert valueBytes != null && valueBytes.length > 0;
        acceptInternal((KeyHandleOffHeap) kh, new HeapData(valueBytes), 0L);
    }

    // called from PartitionOperationThread
    @Override
    public void acceptTombstone(KeyHandle kh, long seq) {
        acceptInternal((KeyHandleOffHeap) kh, null, seq);
    }

    private void acceptInternal(KeyHandleOffHeap keyHandle, HeapData value, long tombstoneSeq) {
        NativeMemoryData key = new NativeMemoryData().reset(keyHandle.address());
        HiDensityNativeMemoryCacheRecord record = fetchedRecordDuringRestart;

        long recordSequence = keyHandle.sequenceId();
        if (record == null) {
            acceptNewRecord(key, value, recordSequence, tombstoneSeq);
        } else {
            assert recordSequence == record.getSequence()
                    : "Expected Seq: " + recordSequence + ", Actual Seq: " + record.getSequence();
            assert record.equals(records.get(key));
            acceptNewValue(record, value, tombstoneSeq);
            fetchedRecordDuringRestart = null;
        }
    }

    private void acceptNewValue(HiDensityNativeMemoryCacheRecord record, HeapData value, long tombstoneSeq) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();

        NativeMemoryData nativeValue = null;
        boolean isTombstone = value == null;

        if (!isTombstone) {
            nativeValue = (NativeMemoryData) recordProcessor.convertData(value, NATIVE);
        }

        long currentValueAddress = record.getValueAddress();
        if (currentValueAddress != NULL_PTR && isTombstone) {
            tombstoneCount++;
        } else if (currentValueAddress == NULL_PTR && !isTombstone) {
            tombstoneCount--;
        }

        recordProcessor.disposeValue(record);
        record.setValue(nativeValue);
        record.setTombstoneSequence(tombstoneSeq);
    }

    private void acceptNewRecord(NativeMemoryData key, HeapData value, long recordSequence, long tombstoneSeq) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        HiDensityNativeMemoryCacheRecord record = null;
        try {
            record = createRecordInternal(value, Clock.currentTimeMillis(), Long.MAX_VALUE, recordSequence);
            record.setTombstoneSequence(tombstoneSeq);
            boolean isNewRecord = records.set(key, record);
            assert isNewRecord;

            if (value == null) {
                tombstoneCount++;
            }
        } catch (NativeOutOfMemoryError e) {
            recordProcessor.disposeData(key);
            if (record != null) {
                recordProcessor.dispose(record);
            }
            throw e;
        }
    }

    @Override
    public int size() {
        return records.size() - tombstoneCount;
    }

    @Override
    public void clear() {
        clearInternal(true);
    }

    private void clearInternal(boolean clearHotRestartStore) {
        if (clearHotRestartStore) {
            hotRestartStore.clear(prefix);
        }

        super.clear();
        cacheInfo.addEntryCount(tombstoneCount);
        tombstoneCount = 0;
    }

    @Override
    public void close() {
        clearInternal(false);
        records.dispose();
        closeListeners();
    }

    private class TombstoneCleanerTask implements PartitionSpecificRunnable {
        private final Collection<TombstoneId> keysToRelease;

        public TombstoneCleanerTask(Collection<TombstoneId> keysToRelease) {
            this.keysToRelease = keysToRelease;
        }

        @Override
        public void run() {
            final NativeMemoryData key = new NativeMemoryData();
            synchronized (recordMapMutex) {
                final HiDensityNativeMemoryCacheRecordMap recordMap = records;
                if (recordMap.capacity() == 0) {
                    // map is disposed
                    return;
                }

                for (TombstoneId toRelease : keysToRelease) {
                    final KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) toRelease.keyHandle();
                    key.reset(keyHandle.address());
                    final HiDensityNativeMemoryCacheRecord record = recordMap.get(key);
                    if (record == null || record.getSequence() != keyHandle.sequenceId()) {
                        continue;
                    }
                    if (record.getTombstoneSequence() != toRelease.tombstoneSeq()) {
                        continue;
                    }
                    if (record.getValueAddress() == NULL_PTR) {
                        recordMap.delete(key);
                        tombstoneCount--;
                    }
                }
            }
            // Helps allow the GC thread to grab the lock in case there
            // are many releaseTombstones operations in a row
            Thread.yield();
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }
    }

    @Override
    public void transferRecordsFrom(ICacheRecordStore src) {
        super.transferRecordsFrom(src);
        if (src instanceof HotRestartHiDensityNativeMemoryCacheRecordStore) {
            this.tombstoneCount = ((HotRestartHiDensityNativeMemoryCacheRecordStore) src).tombstoneCount;
        }
    }
}
