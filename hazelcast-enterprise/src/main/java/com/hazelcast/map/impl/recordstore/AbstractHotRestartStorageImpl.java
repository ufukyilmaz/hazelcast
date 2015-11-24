package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractHotRestartStorageImpl<R extends Record> implements Storage<Data, R>, HotRestartStorage<R> {

    protected final EnterpriseMapServiceContext mapServiceContext;

    protected final HotRestartStore hotRestartStore;

    protected final Storage<Data, R> storage;

    protected final long prefix;

    protected int size;

    public AbstractHotRestartStorageImpl(EnterpriseMapServiceContext mapServiceContext, RecordFactory recordFactory,
                                         InMemoryFormat inMemoryFormat, long prefix) {
        this.mapServiceContext = mapServiceContext;
        this.hotRestartStore = getHotRestartStore();
        this.storage = createStorage(recordFactory, inMemoryFormat);
        this.prefix = prefix;
    }

    public abstract HotRestartStore getHotRestartStore();

    public abstract Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat);

    public abstract HotRestartKey createHotRestartKey(R record);

    public abstract void putInternal(Data key, R record);

    public abstract void updateInternal(Data key, R record, Object value);

    public abstract void removeInternal(R record, long tombstoneSequenceId);

    @Override
    public final void put(Data key, R record) {
        putInternal(key, record);
        putToHotRestart(record);
        size++;
    }

    @Override
    public final void updateRecordValue(Data key, R record, Object value) {
        updateInternal(key, record, value);
        putToHotRestart(record);
    }

    @Override
    public final void removeRecord(R record) {
        if (record == null) {
            return;
        }
        record = get(record.getKey());
        if (record == null) {
            return;
        }
        HotRestartKey hotRestartKey = createHotRestartKey(record);
        long tombstoneSequenceId = hotRestartStore.removeStep1(hotRestartKey);
        removeInternal(record, tombstoneSequenceId);
        hotRestartStore.removeStep2();
        size--;
    }

    @Override
    public void putTransient(Data key, R record) {
        storage.put(key, record);
        if (!record.isTombstone()) {
            size++;
        }
    }

    @Override
    public void updateTransient(Data key, R record, Object value) {
        if (record.isTombstone() && value != null) {
            size++;
        } else if (!record.isTombstone() && value == null) {
            size--;
            assert size >= 0;
        }
        storage.updateRecordValue(key, record, value);
    }

    @Override
    public void removeTransient(R record) {
        storage.removeRecord(record);
    }

    @Override
    public R getRecord(Data key) {
        return storage.get(key);
    }

    @Override
    public R get(Data key) {
        R record = storage.get(key);
        if (record == null || record.isTombstone()) {
            return null;
        }
        return record;
    }

    @Override
    public boolean containsKey(Data key) {
        return get(key) != null;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Collection<R> values() {
        return size == 0 ? Collections.<R>emptyList() : new UnmodifiableTombstoneAwareCollection<R>(storage.values(), size);
    }

    @Override
    public void clear() {
        storage.clear();
        hotRestartStore.clear(prefix);
        size = 0;
    }

    @Override
    public void destroy() {
        storage.destroy();
        hotRestartStore.clear(prefix);
        size = 0;
    }

    @Override
    public SizeEstimator getSizeEstimator() {
        return storage.getSizeEstimator();
    }

    @Override
    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        storage.setSizeEstimator(sizeEstimator);
    }

    @Override
    public void dispose() {
        storage.dispose();
    }

    private void putToHotRestart(R record) {
        HotRestartKey hotRestartKey = createHotRestartKey(record);
        Data value = mapServiceContext.toData(record.getValue());
        hotRestartStore.put(hotRestartKey, value.toByteArray());
    }
}
