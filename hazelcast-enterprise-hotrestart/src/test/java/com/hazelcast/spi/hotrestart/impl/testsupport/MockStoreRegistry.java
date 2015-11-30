package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOnHeapHotRestartStore;

public class MockStoreRegistry implements RamStoreRegistry {
    private static final long TOMBSTONE_RELEASE_INTERVAL_MILLIS = 10;
    public final HotRestartStore hrStore;
    public final ConcurrentMap<Long, MockRecordStore> recordStores = new ConcurrentHashMap<Long, MockRecordStore>();
    private final MemoryAllocator malloc;
    private long lastReleasedTombstones;

    public MockStoreRegistry(HotRestartStoreConfig cfg, MemoryAllocator malloc) {
        this.malloc = malloc;
        cfg.setRamStoreRegistry(this);
        this.hrStore = malloc != null ? newOffHeapHotRestartStore(cfg) : newOnHeapHotRestartStore(cfg);
        hrStore.hotRestart(false);
    }

    public boolean isEmpty() {
        return recordStores.isEmpty();
    }

    public void put(long prefix, long key, byte[] value) {
        getOrCreateRecordStoreForPrefix(prefix).put(key, value);
    }

    public void remove(long prefix, long key) {
        getOrCreateRecordStoreForPrefix(prefix).remove(key);
    }

    public void clear(long[] prefixes) {
        hrStore.clear(prefixes);
        for (long prefix : prefixes) {
            getOrCreateRecordStoreForPrefix(prefix).clear();
        }
    }

    public void releaseTombstonesAsNeeded() {
        if (System.currentTimeMillis() - lastReleasedTombstones < TOMBSTONE_RELEASE_INTERVAL_MILLIS) {
            return;
        }
        for (MockRecordStore s : recordStores.values()) {
            s.drainTombstoneReleaseQueue();
        }
        lastReleasedTombstones = System.currentTimeMillis();
    }

    @Override public RamStore ramStoreForPrefix(long prefix) {
        return recordStores.get(prefix);
    }

    @Override public RamStore restartingRamStoreForPrefix(long prefix) {
        return getOrCreateRecordStoreForPrefix((int) prefix);
    }

    public void closeHotRestartStore() {
        hrStore.close();
    }

    public void disposeRecordStores() {
        for (MockRecordStore s : recordStores.values()) {
            s.dispose();
        }
    }

    MockRecordStore getOrCreateRecordStoreForPrefix(long prefix) {
        MockRecordStore ret = recordStores.get(prefix);
        if (ret == null) {
            ret = malloc != null ? new MockRecordStoreOffHeap(prefix, malloc, hrStore)
                                 : new MockRecordStoreOnHeap(prefix, hrStore);
            final MockRecordStore existing = recordStores.putIfAbsent(prefix, ret);
            if (existing != null) {
                ret = existing;
            }
        }
        return ret;
    }
}
