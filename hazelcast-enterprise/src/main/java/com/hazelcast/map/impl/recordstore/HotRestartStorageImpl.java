package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;

/**
 * HotRestart storage implementation for maps
 * not configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE} please see {@link HotRestartHDStorageImpl}
 */
public class HotRestartStorageImpl<R extends Record> extends AbstractHotRestartStorageImpl<R> {

    HotRestartStorageImpl(RecordFactory<R> recordFactory, InMemoryFormat inMemoryFormat,
                          EnterpriseMapServiceContext mapServiceContext, long prefix) {
        super(mapServiceContext, recordFactory, inMemoryFormat, prefix);
    }

    @Override
    public HotRestartStore getHotRestartStore() {
        return mapServiceContext.getOnHeapHotRestartStoreForCurrentThread();
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat) {
        return new StorageImpl<R>(recordFactory, inMemoryFormat);
    }

    @Override
    public void putInternal(Data key, R record) {
        storage.put(key, record);
    }

    @Override
    public void updateInternal(Data key, R record, Object value) {
        storage.updateRecordValue(key, record, value);
    }

    @Override
    public void removeInternal(R record) {
        StorageImpl storageImpl = (StorageImpl) storage;
        storageImpl.updateSizeEstimator(-storageImpl.calculateHeapCost(record.getValue()));
        record.invalidate();
    }

    @Override
    public HotRestartKey createHotRestartKey(R record) {
        Data key = record.getKey();
        return new KeyOnHeap(prefix, key.toByteArray());
    }
}
