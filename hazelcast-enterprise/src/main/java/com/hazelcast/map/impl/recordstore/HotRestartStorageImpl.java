package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.Iterator;

/**
 * Hot Restart storage implementation for maps not configured with
 * in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * please see {@link HotRestartHDStorageImpl}.
 */
public class HotRestartStorageImpl<R extends Record> implements Storage<Data, R>, HotRestartStorage<R> {

    protected final EnterpriseMapServiceContext mapServiceContext;
    protected final HotRestartStore hotRestartStore;
    protected final Storage<Data, R> storage;
    protected final boolean fsync;
    protected final long prefix;

    HotRestartStorageImpl(EnterpriseMapServiceContext mapServiceContext, RecordFactory<R> recordFactory,
                          InMemoryFormat inMemoryFormat, boolean fsync, long prefix, int partitionId) {
        this.mapServiceContext = mapServiceContext;
        this.fsync = fsync;
        this.hotRestartStore = getHotRestartStore(partitionId);
        this.storage = createStorage(recordFactory, inMemoryFormat);
        this.prefix = prefix;
    }

    public HotRestartStore getHotRestartStore(int partitionId) {
        return mapServiceContext.getOnHeapHotRestartStoreForPartition(partitionId);
    }

    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat inMemoryFormat) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        return new StorageImpl<R>(recordFactory, inMemoryFormat, serializationService);
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
        hotRestartStore.remove(createHotRestartKey(record), fsync);
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        storage.clear(isDuringShutdown);
        if (!isDuringShutdown) {
            hotRestartStore.clear(fsync, prefix);
        }
    }

    @Override
    public void destroy(boolean isDuringShutdown) {
        storage.destroy(isDuringShutdown);
        if (!isDuringShutdown) {
            hotRestartStore.clear(fsync, prefix);
        }
    }

    @Override
    public final R get(Data key) {
        return storage.get(key);
    }

    @Override
    public R getIfSameKey(Data key) {
        return storage.getIfSameKey(key);
    }

    @Override
    public final boolean containsKey(Data key) {
        return storage.containsKey(key);
    }

    @Override
    public Collection<R> values() {
        return storage.values();
    }

    @Override
    public final Iterator<R> mutationTolerantIterator() {
        return storage.mutationTolerantIterator();
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
    public final EntryCostEstimator getEntryCostEstimator() {
        return storage.getEntryCostEstimator();
    }

    @Override
    public final void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        storage.setEntryCostEstimator(entryCostEstimator);
    }

    @Override
    public void disposeDeferredBlocks() {
        storage.disposeDeferredBlocks();
    }

    @Override
    public Iterable<EntryView> getRandomSamples(int sampleCount) {
        return storage.getRandomSamples(sampleCount);
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        return storage.fetchKeys(tableIndex, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size, SerializationService serializationService) {
        return storage.fetchEntries(tableIndex, size, serializationService);
    }

    @Override
    public Record extractRecordFromLazy(EntryView entryView) {
        return storage.extractRecordFromLazy(entryView);
    }

    public HotRestartKey createHotRestartKey(R record) {
        Data key = record.getKey();
        return new KeyOnHeap(prefix, key.toByteArray());
    }

    protected final void putToHotRestart(R record) {
        HotRestartKey hotRestartKey = createHotRestartKey(record);
        Data value = mapServiceContext.toData(record.getValue());
        hotRestartStore.put(hotRestartKey, value.toByteArray(), fsync);
    }
}
