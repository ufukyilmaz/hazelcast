package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.impl.maxsize.impl.CompositeMaxSizeChecker;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOffHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.util.Clock;

import java.util.Iterator;

import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;
import static com.hazelcast.nio.serialization.DataType.NATIVE;
import static java.lang.Math.max;

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
    private final Object recordMapMutex;

    private HiDensityNativeMemoryCacheRecord fetchedRecordDuringRestart;

    public HotRestartHiDensityNativeMemoryCacheRecordStore(int partitionId, String name, EnterpriseCacheService cacheService,
                                                           NodeEngine nodeEngine, boolean fsync, long keyPrefix) {
        super(partitionId, name, cacheService, nodeEngine);
        this.fsync = fsync;
        this.prefix = keyPrefix;
        this.hotRestartStore = cacheService.offHeapHotRestartStoreForCurrentThread();
        assert hotRestartStore != null;

        HotRestartHiDensityNativeMemoryCacheRecordMap recordMap = (HotRestartHiDensityNativeMemoryCacheRecordMap) records;
        recordMapMutex = recordMap.getMutex();
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
    protected MaxSizeChecker createCacheMaxSizeChecker(int size, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        // Max-Size checker is created before internal map,
        // so in case of failure because of invalid max-size policy,
        // since there is no allocated native memory yet,
        // there is no need to free allocated memory.

        int minFreeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();
        boolean skipConfiguredMaxSizeChecker = false;

        if (EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE == maxSizePolicy) {
            // TODO
            // Check is done while creating cache record store.
            // Should we do it while creating cache (proxy) or somewhere else?

            if (size < minFreeNativeMemoryPercentage) {
                throw new IllegalArgumentException("Free native memory percentage cannot be less than "
                        + minFreeNativeMemoryPercentage + "%");
            }

            /*
             * We will also apply `FREE_NATIVE_MEMORY_PERCENTAGE` based max-size policy
             * with size `minFreeNativeMemoryPercentage (20%)` and
             * configuring `FREE_NATIVE_MEMORY_PERCENTAGE` based max-size policy must be bigger than
             * `minFreeNativeMemoryPercentage (20%)`. So no need to two different
             * `FREE_NATIVE_MEMORY_PERCENTAGE` based max-size policy here.
             * Therefore skipping configured `FREE_NATIVE_MEMORY_PERCENTAGE` based max-size policy and
             * using default `FREE_NATIVE_MEMORY_PERCENTAGE` based max-size policy
             * with size `minFreeNativeMemoryPercentage (20%)`.
             */
            skipConfiguredMaxSizeChecker = true;
            minFreeNativeMemoryPercentage = max(minFreeNativeMemoryPercentage, size);

            // TODO Should we log here about skipping the configured one.
            // But since this log is printed for every partition of cache, it might cause lots of log message.
        }

        final long maxNativeMemory =
                ((EnterpriseSerializationService) nodeEngine.getSerializationService())
                        .getMemoryManager().getMemoryStats().getMaxNativeMemory();
        MaxSizeChecker freeNativeMemoryMaxSizeChecker =
                new HiDensityFreeNativeMemoryPercentageMaxSizeChecker(
                        memoryManager, minFreeNativeMemoryPercentage, maxNativeMemory);
        if (skipConfiguredMaxSizeChecker) {
            return freeNativeMemoryMaxSizeChecker;
        } else {
            MaxSizeChecker maxSizeChecker = super.createCacheMaxSizeChecker(size, maxSizePolicy);
            return CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                    CompositeMaxSizeChecker.CompositionOperator.OR,
                    maxSizeChecker,
                    freeNativeMemoryMaxSizeChecker);
        }
    }

    @Override
    long newSequence() {
        return memoryManager.newSequence();
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
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        lookupAndRemoveFromHotRestart(key);
        return super.removeRecord(key);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord doRemoveRecord(Data key, String source) {
        lookupAndRemoveFromHotRestart(key);
        return super.doRemoveRecord(key, source);
    }

    private void lookupAndRemoveFromHotRestart(Data key) {
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (isMemoryBlockValid(record)) {
            removeFromHotRestart(key, record);
        }
    }

    private void fsyncIfRequired() {
        if (fsync) {
            hotRestartStore.fsync();
        }
    }

    @Override
    protected void onOwn(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                         NativeMemoryData oldValueData, boolean isNewPut, boolean disableDeferredDispose) {
        putToHotRestart(key, record);
        super.onOwn(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose);
    }

    private void putToHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        NativeMemoryData value = record.getValue();
        assert value != null : "Value should not be null! -> " + record;
        byte[] valueBytes = value.toByteArray();
        hotRestartStore.put(newHotRestartKey(key, record), valueBytes, fsync);
        fsyncIfRequired();
    }

    private void removeFromHotRestart(Data key, HiDensityNativeMemoryCacheRecord record) {
        final KeyOffHeap hotRestartKey = newHotRestartKey(key, record);
        hotRestartStore.remove(hotRestartKey, fsync);
        fsyncIfRequired();
    }

    private KeyOffHeap newHotRestartKey(Data key, HiDensityNativeMemoryCacheRecord record) {
        long keyAddress = records.getNativeKeyAddress(key);
        assert keyAddress != NULL_PTR : "Invalid key address!";
        assert record.address() != NULL_PTR;
        assert record.getValueAddress() != NULL_PTR;
        return new KeyOffHeap(prefix, key.toByteArray(), keyAddress, record.getSequence());
    }

    // called from hotrestart GC thread
    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) {
        KeyHandleOffHeap kh = (KeyHandleOffHeap) keyHandle;
        assert kh.address() != NULL_PTR;

        synchronized (recordMapMutex) {
            NativeMemoryData key = RamStoreHelper.validateAndGetKey(kh, memoryManager);
            if (key == null) {
                return false;
            }
            HiDensityNativeMemoryCacheRecord record = records.get(key);
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

    private KeyHandleOffHeap newKeyHandle(HeapData heapKey) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        NativeMemoryData nativeKey = (NativeMemoryData) recordProcessor.convertData(heapKey, NATIVE);
        long recordSequence = newSequence();
        // fetchedRecordDuringRestart will be used in #accept() method
        fetchedRecordDuringRestart = acceptNewRecord(nativeKey, recordSequence);
        return new SimpleHandleOffHeap(nativeKey.address(), recordSequence);
    }

    private HiDensityNativeMemoryCacheRecord acceptNewRecord(NativeMemoryData key, long recordSequence) {
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
        return record;
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

        final KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) kh;

        HiDensityNativeMemoryCacheRecord record = fetchedRecordDuringRestart;
        long recordSequence = keyHandle.sequenceId();

        assert record != null;
        assert recordSequence == record.getSequence()
                : "Expected Seq: " + recordSequence + ", Actual Seq: " + record.getSequence();

        acceptNewValue(record, new HeapData(valueBytes));
        fetchedRecordDuringRestart = null;
    }

    private void acceptNewValue(HiDensityNativeMemoryCacheRecord record, HeapData value) {
        HiDensityRecordProcessor recordProcessor = getRecordProcessor();
        NativeMemoryData nativeValue = (NativeMemoryData) recordProcessor.convertData(value, NATIVE);

        recordProcessor.disposeValue(record);
        record.setValue(nativeValue);
    }

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

    private void scanEmptyRecords() {
        Iterator<HiDensityNativeMemoryCacheRecord> iter = records.valueIter();
        while (iter.hasNext()) {
            HiDensityNativeMemoryCacheRecord record = iter.next();
            assert record != null;
            assert record.getValueAddress() != NULL_PTR;
        }
    }

    @Override
    public void clear() {
        clearInternal(true);
    }

    private void clearInternal(boolean clearHotRestartStore) {
        if (clearHotRestartStore) {
            hotRestartStore.clear(prefix);
            fsyncIfRequired();
        }
        super.clear();
    }

    @Override
    public void close(boolean onShutdown) {
        if (shouldExplicitlyClear(onShutdown)) {
            clearInternal(false);
        }
        records.dispose();
        closeListeners();
    }
}
