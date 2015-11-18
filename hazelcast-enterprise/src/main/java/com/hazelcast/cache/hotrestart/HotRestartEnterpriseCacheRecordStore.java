package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.DefaultEnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreHelper;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;

import java.util.Collection;

/**
 * On-heap cache record store with Hot Restart support.
 */
public class HotRestartEnterpriseCacheRecordStore extends DefaultEnterpriseCacheRecordStore implements RamStore {

    private final long prefix;
    private final HotRestartStore hotRestartStore;
    private int tombstoneCount;

    public HotRestartEnterpriseCacheRecordStore(
            String name, int partitionId, NodeEngine nodeEngine, EnterpriseCacheService cacheService, long keyPrefix) {
        super(name, partitionId, nodeEngine, cacheService);
        this.prefix = keyPrefix;
        this.hotRestartStore = cacheService.onHeapHotRestartStoreForCurrentThread();
    }

    @Override
    protected CacheRecordHashMap createRecordCacheMap() {
        return new HotRestartCacheRecordHashMap(DEFAULT_INITIAL_CAPACITY, cacheContext);
    }

    @Override
    protected CacheRecord doPutRecord(Data key, CacheRecord record, String source) {
        CacheRecord oldRecord = super.doPutRecord(key, record, source);
        putToHotRestart(key, record.getValue());
        return oldRecord;
    }

    @Override
    protected void onUpdateRecord(Data key, CacheRecord record, Object value, Data oldDataValue) {
        super.onUpdateRecord(key, record, value, oldDataValue);
        putToHotRestart(key, record.getValue());
        if (oldDataValue == null) {
            tombstoneCount--;
            record.setTombstoneSequence(0);
            cacheContext.increaseEntryCount();
        }
    }

    @Override
    protected void onRemove(Data key, Object value, String source, boolean getValue, CacheRecord record,
                            boolean removed) {
        super.onRemove(key, value, source, getValue, record, removed);
        if (removed) {
            removeFromHotRestart(key, record);
        }
    }

    @Override
    public void onEvict(Data key, CacheRecord record) {
        super.onEvict(key, record);
        removeFromHotRestart(key, record);
    }

    @Override
    protected void onProcessExpiredEntry(Data key, CacheRecord record, long expiryTime, long now, String source, String origin) {
        super.onProcessExpiredEntry(key, record, expiryTime, now, source, origin);
        removeFromHotRestart(key, record);
    }

    // called from Hot Restart GC thread
    @Override
    public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink sink) {
        final KeyOnHeap keyHandle = (KeyOnHeap) kh;
        byte[] keyBytes = keyHandle.bytes();
        Data key = new HeapData(keyBytes);
        final CacheRecord record = records.get(key);
        if (record == null) {
            throw new HotRestartException("Record not found! Handle: " + keyHandle);
        }
        Data value = record.isTombstone() ? null : toData(record.getValue());
        return RamStoreHelper.copyEntry(keyHandle, value, expectedSize, sink);
    }

    // called from Hot Restart GC thread
    @Override
    public void releaseTombstones(final Collection<TombstoneId> keysToRelease) {
        InternalOperationService opService = (InternalOperationService) nodeEngine.getOperationService();
        opService.execute(new TombstoneCleanerTask(keysToRelease));
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
        CacheRecord oldRecord = records.put(key, record);
        if (oldRecord != null && oldRecord.getValue() == null) {
            tombstoneCount--;
        }
    }

    // called from PartitionOperationThread
    @Override
    public void acceptTombstone(KeyHandle kh, long seq) {
        KeyOnHeap heavyKey = (KeyOnHeap) kh;
        HeapData keyData = new HeapData(heavyKey.bytes());
        CacheRecord record = cacheRecordFactory.newRecordWithExpiry(null, seq, -1L);
        records.put(keyData, record);
        tombstoneCount++;
    }

    private void putToHotRestart(Data key, Object value) {
        byte[] keyBytes = key.toByteArray();
        byte[] valueBytes = serializationService.toData(value).toByteArray();
        hotRestartStore.put(new KeyOnHeap(prefix, keyBytes), valueBytes);
    }

    private void removeFromHotRestart(Data key, CacheRecord record) {
        final byte[] keyBytes = key.toByteArray();
        final HotRestartStore store = hotRestartStore;
        final long tombstoneSeq = store.removeStep1(new KeyOnHeap(prefix, keyBytes));
        record.setTombstoneSequence(tombstoneSeq);
        record.setValue(null);
        // put back record as tombstone
        // Hotrestart will call #releaseTombstone() to actually remove the record
        records.put(key, record);
        store.removeStep2();
        tombstoneCount++;
        cacheContext.decreaseEntryCount();
    }

    @Override
    public int size() {
        return records.size() - tombstoneCount;
    }

    @Override
    public void clear() {
        clearInternal(true);
    }

    private void clearInternal(boolean clearHotRestartStore) {
        super.clear();

        cacheContext.increaseEntryCount(tombstoneCount);
        tombstoneCount = 0;

        if (clearHotRestartStore) {
            hotRestartStore.clear(prefix);
        }
    }

    @Override
    public void close() {
        clearInternal(false);
        closeListeners();
    }

    private class TombstoneCleanerTask implements PartitionSpecificRunnable {
        private final Collection<TombstoneId> keysToRelease;

        public TombstoneCleanerTask(Collection<TombstoneId> keysToRelease) {
            this.keysToRelease = keysToRelease;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            for (TombstoneId toRelease : keysToRelease) {
                KeyOnHeap keyOnHeap = (KeyOnHeap) toRelease.keyHandle();
                HeapData key = new HeapData(keyOnHeap.bytes());
                CacheRecord record = records.get(key);
                if (record == null || record.getValue() != null) {
                    continue;
                }
                if (record.getTombstoneSequence() == toRelease.tombstoneSeq()) {
                    records.remove(key, record);
                    tombstoneCount--;
                }
            }
        }
    }

    @Override
    public void transferRecordsFrom(ICacheRecordStore src) {
        super.transferRecordsFrom(src);
        if (src instanceof HotRestartEnterpriseCacheRecordStore) {
            this.tombstoneCount = ((HotRestartEnterpriseCacheRecordStore) src).tombstoneCount;
        }
    }
}
