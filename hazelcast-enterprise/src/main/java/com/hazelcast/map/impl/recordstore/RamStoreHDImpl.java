package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import java.util.Collection;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * RamStore implementation for maps configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 */
public class RamStoreHDImpl extends AbstractRamStoreImpl {

    private final HotRestartHDStorageImpl storage;

    private final Object mutex;

    public RamStoreHDImpl(EnterpriseRecordStore recordStore) {
        super(recordStore);
        this.storage = (HotRestartHDStorageImpl) recordStore.getStorage();
        this.mutex = storage.getMutex();
    }

    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) throws HotRestartException {
        KeyHandleOffHeap kh = (KeyHandleOffHeap) keyHandle;
        synchronized (mutex) {
            NativeMemoryData key = new NativeMemoryData().reset(kh.address());
            HDRecord record = storage.getRecord(key);
            if (record == null) {
                return false;
            }
            return RamStoreHelper.copyEntry(kh, key, record, expectedSize, sink);
        }
    }

    @Override
    public void releaseTombstonesInternal(Collection<TombstoneId> keysToRelease) {
        synchronized (mutex) {
            final NativeMemoryData key = new NativeMemoryData();
            for (TombstoneId toRelease : keysToRelease) {
                KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) toRelease.keyHandle();
                key.reset(keyHandle.address());
                HDRecord record = storage.getRecord(key);
                if (record.getSequence() != keyHandle.sequenceId()
                        || record.getTombstoneSequence() != toRelease.tombstoneSeq()) {
                    continue;
                }
                if (record.isTombstone()) {
                    storage.removeTransient(record);
                }
            }
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
    public Data createKey(KeyHandle kh) {
        KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) kh;
        return new NativeMemoryData().reset(keyHandle.address());
    }

    @Override
    public HDRecord createRecord(KeyHandle kh, Data value) {
        KeyHandleOffHeap keyHandle = (KeyHandleOffHeap) kh;
        return recordStore.createHDRecord(value, keyHandle.sequenceId());
    }

    private KeyHandleOffHeap readKeyHandle(long nativeKeyAddress) {
        NativeMemoryData keyData = new NativeMemoryData().reset(nativeKeyAddress);
        HDRecord record = storage.getRecord(keyData);
        return new SimpleHandleOffHeap(keyData.address(), record.getSequence());
    }

    private KeyHandleOffHeap newKeyHandle(byte[] key) {
        NativeMemoryData keyData = storage.toNative(new HeapData(key));
        return new SimpleHandleOffHeap(keyData.address(), recordStore.incrementSequence());
    }
}
