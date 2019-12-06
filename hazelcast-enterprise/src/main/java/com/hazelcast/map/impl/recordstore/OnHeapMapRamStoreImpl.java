package com.hazelcast.map.impl.recordstore;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreHelper;
import com.hazelcast.internal.hotrestart.RecordDataSink;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;

/**
 * On heap map's RamStore implementation. A map is on heap if it's
 * in-memory format is {@link com.hazelcast.config.InMemoryFormat#BINARY}
 * or {@link com.hazelcast.config.InMemoryFormat#OBJECT}
 *
 * @see HDMapRamStoreImpl
 */
public class OnHeapMapRamStoreImpl implements RamStore {

    private final EnterpriseRecordStore recordStore;

    private final HotRestartStorageImpl<Record> storage;

    public OnHeapMapRamStoreImpl(EnterpriseRecordStore recordStore) {
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
