package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.util.Clock;

/**
 * RamStore implementation for maps
 * not configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE} please see {@link RamStoreHDImpl}.
 */
public class RamStoreImpl implements RamStore {

    private final EnterpriseRecordStore recordStore;

    private final HotRestartStorageImpl<Record> storage;

    public RamStoreImpl(EnterpriseRecordStore recordStore) {
        this.recordStore = recordStore;
        this.storage = (HotRestartStorageImpl<Record>) recordStore.getStorage();
    }

    @Override
    public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink sink) throws HotRestartException {
        Data key = new HeapData(((KeyOnHeap) kh).bytes());
        Record record = storage.get(key);
        if (record == null) {
            return false;
        }
        Data value = recordStore.toData(record.getValue());
        return RamStoreHelper.copyEntry((KeyOnHeap) kh, value, expectedSize, sink);
    }

    @Override
    public KeyHandle toKeyHandle(byte[] key) {
        return new KeyOnHeap(recordStore.getPrefix(), key);
    }

    @Override
    public void removeNullEntries(SetOfKeyHandle keyHandles) {
        // we don't keep tombstones during restart
    }

    @Override
    public void accept(KeyHandle kh, byte[] valueBytes) {
        HeapData value = new HeapData(valueBytes);
        HotRestartStorage<Record> storage = (HotRestartStorage) recordStore.getStorage();
        Data key = new HeapData(((KeyOnHeap) kh).bytes());
        Record record = storage.get(key);
        if (record == null) {
            record = recordStore.createRecord(key, value, -1, -1, Clock.currentTimeMillis());
            storage.putTransient(key, record);
        } else {
            storage.updateTransient(key, record, value);
        }
        recordStore.disposeDeferredBlocks();
    }
}
