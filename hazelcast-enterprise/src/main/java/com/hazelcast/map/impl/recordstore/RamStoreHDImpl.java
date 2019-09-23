package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * RamStore implementation for maps configured with in-memory-format:
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 */
public class RamStoreHDImpl implements RamStore {

    private static final int REMOVE_NULL_ENTRIES_BATCH_SIZE = 1024;

    private final EnterpriseRecordStore recordStore;

    private final HazelcastMemoryManager memoryManager;

    private final HotRestartHDStorageImpl storage;

    private final Object mutex;

    public RamStoreHDImpl(EnterpriseRecordStore recordStore, HazelcastMemoryManager memoryManager) {
        this.recordStore = recordStore;
        this.memoryManager = memoryManager;
        this.storage = (HotRestartHDStorageImpl) recordStore.getStorage();
        this.mutex = storage.getMutex();
    }

    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) throws HotRestartException {
        KeyHandleOffHeap kh = (KeyHandleOffHeap) keyHandle;
        synchronized (mutex) {
            NativeMemoryData key = RamStoreHelper.validateAndGetKey(kh, memoryManager);
            if (key == null) {
                return false;
            }
            HDRecord record = storage.getIfSameKey(key);
            return record != null && RamStoreHelper.copyEntry(kh, key, record, expectedSize, sink);
        }
    }

    @Override
    public KeyHandle toKeyHandle(byte[] key) {
        HeapData keyData = new HeapData(key);
        long nativeKeyAddress = storage.getNativeKeyAddress(keyData);
        if (nativeKeyAddress != NULL_ADDRESS) {
            return readKeyHandle(nativeKeyAddress);
        }
        return newKeyHandle(key);
    }

    @Override
    public void removeNullEntries(SetOfKeyHandle keyHandles) {
        SetOfKeyHandle.KhCursor cursor = keyHandles.cursor();
        NativeMemoryData key = new NativeMemoryData();
        long removedCount = 0;
        while (cursor.advance()) {
            KeyHandleOffHeap keyHandleOffHeap = (KeyHandleOffHeap) cursor.asKeyHandle();
            key.reset(keyHandleOffHeap.address());
            HDRecord record = storage.get(key);
            assert record != null;
            assert record.getSequence() == keyHandleOffHeap.sequenceId();
            storage.removeTransient(record);
            if (++removedCount % REMOVE_NULL_ENTRIES_BATCH_SIZE == 0) {
                storage.disposeDeferredBlocks();
            }
        }
        storage.disposeDeferredBlocks();
    }

    private KeyHandleOffHeap readKeyHandle(long nativeKeyAddress) {
        NativeMemoryData keyData = new NativeMemoryData().reset(nativeKeyAddress);
        HDRecord record = storage.get(keyData);
        return new SimpleHandleOffHeap(keyData.address(), record.getSequence());
    }

    private KeyHandleOffHeap newKeyHandle(byte[] key) {
        NativeMemoryData keyData = storage.toNative(new HeapData(key));
        long sequenceId = recordStore.incrementSequence();
        SimpleHandleOffHeap handleOffHeap = new SimpleHandleOffHeap(keyData.address(), sequenceId);
        HDRecord record = recordStore.createRecord(keyData, null, handleOffHeap.sequenceId());
        storage.putTransient(keyData, record);
        return handleOffHeap;
    }

    @Override
    public void accept(KeyHandle kh, byte[] valueBytes) {
        HeapData value = new HeapData(valueBytes);
        HotRestartStorage<Record> storage = (HotRestartStorage) recordStore.getStorage();
        KeyHandleOffHeap keyHandleOffHeap = (KeyHandleOffHeap) kh;
        Data key = new NativeMemoryData().reset(keyHandleOffHeap.address());
        Record record = storage.get(key);
        assert record != null;
        storage.updateTransient(key, record, value);
        recordStore.disposeDeferredBlocks();
    }
}
