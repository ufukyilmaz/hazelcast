package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOnHeapHotRestartStore;

class MockStoreRegistry implements RamStoreRegistry {
    final ConcurrentMap<Long, MockRecordStore> recordStores = new ConcurrentHashMap<Long, MockRecordStore>();
    final HotRestartStore hrStore;
    private final boolean offHeap;

    MockStoreRegistry(HotRestartStoreConfig cfg) {
        this.offHeap = cfg.malloc() != null;
        cfg.setRamStoreRegistry(this);
        this.hrStore = offHeap ? newOffHeapHotRestartStore(cfg) : newOnHeapHotRestartStore(cfg);
        hrStore.hotRestart(false);
    }

    boolean isEmpty() {
        return recordStores.isEmpty();
    }

    void put(long prefix, byte[] key, byte[] value) {
        getOrCreateRecordStoreForPrefix(prefix).put(key, value);
    }

    void remove(long prefix, byte[] key) {
        getOrCreateRecordStoreForPrefix(prefix).remove(key);
    }

    void clear(long prefix) {
        getOrCreateRecordStoreForPrefix(prefix).clear();
    }

    @Override public RamStore ramStoreForPrefix(long prefix) {
        return recordStores.get(prefix);
    }

    @Override public RamStore restartingRamStoreForPrefix(long prefix) {
        return getOrCreateRecordStoreForPrefix((int) prefix);
    }

    void close() {
        hrStore.close();
    }

    MockRecordStore getOrCreateRecordStoreForPrefix(long prefix) {
        MockRecordStore ret = recordStores.get(prefix);
        if (ret == null) {
            ret = offHeap ? new MockRecordStoreOffHeap(prefix, hrStore) : new MockRecordStoreOnHeap(prefix, hrStore);
            final MockRecordStore existing = recordStores.putIfAbsent(prefix, ret);
            if (existing != null) {
                ret = existing;
            }
        }
        return ret;
    }
}
