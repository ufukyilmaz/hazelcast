package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.util.Clock;

/**
 * RamStore implementation for maps
 * not configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE} please see {@link RamStoreHDImpl}
 */
public class RamStoreImpl extends AbstractRamStoreImpl {

    private final HotRestartStorageImpl<Record> storage;

    public RamStoreImpl(EnterpriseRecordStore recordStore) {
        super(recordStore);
        this.storage = (HotRestartStorageImpl<Record>) recordStore.getStorage();
    }

    @Override
    public boolean copyEntry(KeyHandle keyHandle, int expectedSize, RecordDataSink sink) throws HotRestartException {
        Data key = new HeapData(((KeyOnHeap) keyHandle).bytes());
        Record record = storage.getRecord(key);
        if (record == null) {
            return false;
        }
        Data value = record.isTombstone() ? null : toData(record.getValue());
        return RamStoreHelper.copyEntry((KeyOnHeap) keyHandle, value, expectedSize, sink);
    }

    @Override
    public Data createKey(KeyHandle kh) {
        return new HeapData(((KeyOnHeap) kh).bytes());
    }

    @Override
    public Record createRecord(KeyHandle kh, Data value) {
        return recordStore.createRecord(value, -1, Clock.currentTimeMillis());
    }

    @Override
    public KeyHandle toKeyHandle(byte[] key) {
        return new KeyOnHeap(recordStore.getPrefix(), key);
    }

    @Override public void removeNullEntries(SetOfKeyHandle keyHandles) {

    }

    private Data toData(Object value) {
        return recordStore.toData(value);
    }

}
