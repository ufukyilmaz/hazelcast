package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;

import java.util.Collection;

/**
 * HotRestart storage implementation for maps
 * not configured with in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE} please see {@link HotRestartHDStorageImpl}
 */
public class HotRestartStorageImpl<R extends Record> implements Storage<Data, R>, HotRestartStorage<R> {

    protected final EnterpriseMapServiceContext mapServiceContext;

    protected final HotRestartStore hotRestartStore;

    protected final Storage<Data, R> storage;

    protected final boolean fsync;

    protected final long prefix;

    HotRestartStorageImpl(EnterpriseMapServiceContext mapServiceContext, RecordFactory<R> recordFactory,
            InMemoryFormat inMemoryFormat, boolean fsync, long prefix) {
        this.mapServiceContext = mapServiceContext;
        this.fsync = fsync;
        this.hotRestartStore = getHotRestartStore();
        this.storage = createStorage(recordFactory, inMemoryFormat);
        this.prefix = prefix;
    }

    public HotRestartStore getHotRestartStore() {
        return mapServiceContext.getOnHeapHotRestartStoreForCurrentThread();
    }

    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat) {
        return new StorageImpl<R>(recordFactory, inMemoryFormat);
    }

    @Override
    public final void putTransient(Data key, R record) {
        storage.put(key, record);
    }

    @Override
    public final void updateTransient(Data key, R record, Object value) {
        storage.updateRecordValue(key, record, value);
    }

    @Override
    public final void removeTransient(R record) {
        storage.removeRecord(record);
    }

    @Override
    public void put(Data key, R record) {
        storage.put(key, record);
        putToHotRestart(record);
    }

    @Override
    public void updateRecordValue(Data key, R record, Object value) {
        storage.updateRecordValue(key, record, value);
        putToHotRestart(record);
    }

    @Override
    public void removeRecord(R record) {
        if (record == null) {
            return;
        }
        storage.removeRecord(record);
        hotRestartStore.remove(createHotRestartKey(record));
        fsyncIfRequired();
    }

    @Override
    public void clear() {
        storage.clear();
        hotRestartStore.clear(prefix);
        fsyncIfRequired();
    }

    @Override
    public void destroy() {
        storage.destroy();
        hotRestartStore.clear(prefix);
        fsyncIfRequired();
    }

    @Override
    public final R get(Data key) {
        return storage.get(key);
    }

    @Override
    public final boolean containsKey(Data key) {
        return storage.containsKey(key);
    }

    @Override
    public final Collection<R> values() {
        return storage.values();
    }

    @Override
    public final int size() {
        return storage.size();
    }

    @Override
    public final boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public final SizeEstimator getSizeEstimator() {
        return storage.getSizeEstimator();
    }

    @Override
    public final void setSizeEstimator(SizeEstimator sizeEstimator) {
        storage.setSizeEstimator(sizeEstimator);
    }

    @Override
    public final void dispose() {
        storage.dispose();
    }

    public HotRestartKey createHotRestartKey(R record) {
        Data key = record.getKey();
        return new KeyOnHeap(prefix, key.toByteArray());
    }

    protected final void putToHotRestart(R record) {
        HotRestartKey hotRestartKey = createHotRestartKey(record);
        Data value = mapServiceContext.toData(record.getValue());
        hotRestartStore.put(hotRestartKey, value.toByteArray());
        fsyncIfRequired();
    }

    protected final void fsyncIfRequired() {
        if (fsync) {
            hotRestartStore.fsync();
        }
    }
}
