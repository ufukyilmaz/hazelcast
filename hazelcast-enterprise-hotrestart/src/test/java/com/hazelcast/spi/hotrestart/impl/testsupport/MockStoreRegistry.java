package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.RamStoreRestartLoop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOnHeapHotRestartStore;

public class MockStoreRegistry implements RamStoreRegistry {

    public final HotRestartStore hrStore;
    public final ConcurrentMap<Long, MockRecordStore> recordStores = new ConcurrentHashMap<Long, MockRecordStore>();
    private final MemoryManager memMgr;
    public final boolean fsyncEnabled;

    public MockStoreRegistry(HotRestartStoreConfig cfg, MemoryManager memMgr, boolean fsyncEnabled) throws InterruptedException {
        this.memMgr = memMgr;
        this.fsyncEnabled = fsyncEnabled;
        cfg.setRamStoreRegistry(this);
        this.hrStore = memMgr != null ? newOffHeapHotRestartStore(cfg) : newOnHeapHotRestartStore(cfg);
        final RamStoreRestartLoop loop = new RamStoreRestartLoop(1, 1, this, cfg.logger());
        final Throwable[] failure = { null };
        final Thread restartIoThread = new Thread() {
            @Override
            public void run() {
                try {
                    hrStore.hotRestart(false, 1, loop.keyReceivers[0], loop.keyHandleSenders[0], loop.valueReceivers[0]);
                } catch (Throwable t) {
                    failure[0] = t;
                }
            }
        };
        restartIoThread.start();
        try {
            loop.run(0);
        } catch (Throwable t) {
            throw new HotRestartException("ramStoreRestartLoop failed", t);
        }
        restartIoThread.join();
        if (failure[0] != null) {
            throw new HotRestartException("hotRestart IO thread failed", failure[0]);
        }
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

    public void clear(long... prefixes) {
        hrStore.clear(fsyncEnabled, prefixes);
        for (long prefix : prefixes) {
            getOrCreateRecordStoreForPrefix(prefix).clear();
        }
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        return recordStores.get(prefix);
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        return getOrCreateRecordStoreForPrefix((int) prefix);
    }

    @Override
    public int prefixToThreadId(long prefix) {
        return 0;
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
            ret = memMgr != null ? new MockRecordStoreOffHeap(prefix, memMgr, hrStore, fsyncEnabled)
                                 : new MockRecordStoreOnHeap(prefix, hrStore, fsyncEnabled);
            final MockRecordStore existing = recordStores.putIfAbsent(prefix, ret);
            if (existing != null) {
                ret = existing;
            }
        }
        return ret;
    }
}
