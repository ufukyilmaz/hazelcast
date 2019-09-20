package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.Clock;

/**
 * On-heap cache record store with Hot Restart support.
 */
public class HotRestartEnterpriseCacheRecordStore extends CacheRecordStore implements RamStore {

    private final long prefix;
    private final boolean fsync;
    private final HotRestartStore hotRestartStore;

    public HotRestartEnterpriseCacheRecordStore(String name, int partitionId, NodeEngine nodeEngine,
                                                EnterpriseCacheService cacheService, boolean fsync, long keyPrefix) {
        super(name, partitionId, nodeEngine, cacheService);
        this.fsync = fsync;
        this.prefix = keyPrefix;
        this.hotRestartStore = cacheService.onHeapHotRestartStoreForPartition(partitionId);
        assert hotRestartStore != null;
    }

    @Override
    protected CacheRecord doPutRecord(Data key, CacheRecord record, String source, boolean updateJournal) {
        CacheRecord oldRecord = super.doPutRecord(key, record, source, updateJournal);
        putToHotRestart(key, record.getValue());
        return oldRecord;
    }

    @Override
    protected void onUpdateRecord(Data key, CacheRecord record, Object value, Data oldDataValue) {
        super.onUpdateRecord(key, record, value, oldDataValue);
        putToHotRestart(key, record.getValue());
    }

    @Override
    protected void onRemove(Data key, Object value, String source, boolean getValue, CacheRecord record,
                            boolean removed) {
        super.onRemove(key, value, source, getValue, record, removed);
        if (removed) {
            removeFromHotRestart(key);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        CacheRecord record = super.removeRecord(key);
        if (record != null) {
            removeFromHotRestart(key);
        }
        return record;
    }

    @Override
    public void onEvict(Data key, CacheRecord record, boolean wasExpired) {
        super.onEvict(key, record, wasExpired);
        removeFromHotRestart(key);
    }

    @Override
    protected void onProcessExpiredEntry(Data key, CacheRecord record, long expiryTime, long now, String source, String origin) {
        super.onProcessExpiredEntry(key, record, expiryTime, now, source, origin);
        removeFromHotRestart(key);
    }

    // called from Hot Restart GC thread
    @Override
    public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink sink) {
        final KeyOnHeap keyHandle = (KeyOnHeap) kh;
        byte[] keyBytes = keyHandle.bytes();
        Data key = new HeapData(keyBytes);
        final CacheRecord record = records.get(key);
        if (record == null) {
            return false;
        }
        Data value = toData(record.getValue());
        return RamStoreHelper.copyEntry(keyHandle, value, expectedSize, sink);
    }

    // called from PartitionOperationThread
    @Override
    public KeyOnHeap toKeyHandle(byte[] key) {
        return new KeyOnHeap(prefix, key);
    }

    // called from PartitionOperationThread
    @Override
    public void accept(KeyHandle kh, byte[] valueBytes) {
        HeapData key = new HeapData(((KeyOnHeap) kh).bytes());
        HeapData value = new HeapData(valueBytes);
        CacheRecord record = cacheRecordFactory.newRecordWithExpiry(value, Clock.currentTimeMillis(), -1L);
        records.put(key, record);
    }

    private void putToHotRestart(Data key, Object value) {
        byte[] keyBytes = key.toByteArray();
        byte[] valueBytes = serializationService.toData(value).toByteArray();
        final KeyOnHeap kh = new KeyOnHeap(prefix, keyBytes);
        hotRestartStore.put(kh, valueBytes, fsync);
    }

    private void removeFromHotRestart(Data key) {
        byte[] keyBytes = key.toByteArray();
        final KeyOnHeap kh = new KeyOnHeap(prefix, keyBytes);
        hotRestartStore.remove(kh, fsync);
    }

    @Override
    public void removeNullEntries(SetOfKeyHandle keyHandles) {
        // we don't keep tombstones during restart
    }

    @Override
    public void reset() {
        resetInternal(true);
    }

    private void resetInternal(boolean clearHotRestartStore) {
        if (clearHotRestartStore) {
            hotRestartStore.clear(fsync, prefix);
        }
        super.reset();
    }

    @Override
    public void close(boolean onShutdown) {
        resetInternal(false);
        destroyEventJournal();
        closeListeners();
    }
}
