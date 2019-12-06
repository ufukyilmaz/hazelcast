package com.hazelcast.map.impl.recordstore;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreHelper;
import com.hazelcast.internal.hotrestart.RecordDataSink;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.internal.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * HD memory backed map's RamStore implementation.
 *
 * @see OnHeapMapRamStoreImpl
 */
public class HDMapRamStoreImpl implements RamStore {

    private static final int REMOVE_NULL_ENTRIES_BATCH_SIZE = 1024;

    private final EnterpriseRecordStore recordStore;
    private final HazelcastMemoryManager memoryManager;
    private final HotRestartHDStorageImpl storage;
    private final Object mutex;

    public HDMapRamStoreImpl(EnterpriseRecordStore recordStore, HazelcastMemoryManager memoryManager) {
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
    public KeyHandle toKeyHandle(byte[] keyBytes) {
        HeapData keyData = new HeapData(keyBytes);
        long nativeKeyAddress = storage.getNativeKeyAddress(keyData);
        if (nativeKeyAddress != NULL_ADDRESS) {
            return readExistingKeyHandle(nativeKeyAddress);
        }
        return newKeyHandle(keyBytes);
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
            storage.removeTransient(key, record);
            if (++removedCount % REMOVE_NULL_ENTRIES_BATCH_SIZE == 0) {
                storage.disposeDeferredBlocks();
            }
        }
        storage.disposeDeferredBlocks();
    }

    private KeyHandleOffHeap readExistingKeyHandle(long nativeKeyAddress) {
        NativeMemoryData keyData = new NativeMemoryData().reset(nativeKeyAddress);
        HDRecord record = storage.get(keyData);
        return new SimpleHandleOffHeap(keyData.address(), record.getSequence());
    }

    private KeyHandleOffHeap newKeyHandle(byte[] keyBytes) {
        NativeMemoryData keyData = storage.toNative(new HeapData(keyBytes));
        long sequenceId = recordStore.incrementSequence();
        SimpleHandleOffHeap handleOffHeap = new SimpleHandleOffHeap(keyData.address(), sequenceId);
        Record record = recordStore.createRecord(keyData, null, Clock.currentTimeMillis());
        record.setSequence(handleOffHeap.sequenceId());
        storage.putTransient(keyData, (HDRecord) record);
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
