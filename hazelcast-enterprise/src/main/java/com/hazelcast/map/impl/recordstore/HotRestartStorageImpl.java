package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.internal.serialization.Data;

import java.util.Iterator;
import java.util.Map;

/**
 * Hot Restart storage implementation for maps not configured with
 * in-memory-format: {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * For {@link com.hazelcast.config.InMemoryFormat#NATIVE}
 * please see {@link HotRestartHDStorageImpl}.
 */
public class HotRestartStorageImpl<R extends Record>
        implements Storage<Data, R>, HotRestartStorage<R> {

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
        return new StorageImpl<R>(inMemoryFormat, serializationService);
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
    public final void removeTransient(Data key, R record) {
        storage.removeRecord(key, record);
    }

    @Override
    public void put(Data key, R record) {
        storage.put(key, record);
        putToHotRestart(key, record);
    }

    @Override
    public void updateRecordValue(Data key, R record, Object value) {
        storage.updateRecordValue(key, record, value);
        putToHotRestart(key, record);
    }

    @Override
    public void removeRecord(Data key, R record) {
        hotRestartStore.remove(createHotRestartKey(key, record), fsync);
        storage.removeRecord(key, record);
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
    public final Iterator<Map.Entry<Data, R>> mutationTolerantIterator() {
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
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        return storage.fetchKeys(pointers, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        return storage.fetchEntries(pointers, size);
    }

    @Override
    public Record extractRecordFromLazy(EntryView evictableEntryView) {
        return storage.extractRecordFromLazy(evictableEntryView);
    }

    @Override
    public Data extractDataKeyFromLazy(EntryView entryView) {
        return storage.extractDataKeyFromLazy(entryView);
    }

    @Override
    public Data toBackingDataKeyFormat(Data key) {
        return storage.toBackingDataKeyFormat(key);
    }

    protected final void putToHotRestart(Data key, R record) {
        HotRestartKey hotRestartKey = createHotRestartKey(key, record);
        Data value = mapServiceContext.toData(record.getValue());
        hotRestartStore.put(hotRestartKey, value.toByteArray(), fsync);
    }

    public HotRestartKey createHotRestartKey(Data key, Record record) {
        return new KeyOnHeap(prefix, key.toByteArray());
    }
}
