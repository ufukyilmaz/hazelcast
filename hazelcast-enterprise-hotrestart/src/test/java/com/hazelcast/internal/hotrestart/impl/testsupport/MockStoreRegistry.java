package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreRegistry;
import com.hazelcast.internal.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.internal.hotrestart.impl.RamStoreRestartLoop;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.hotrestart.impl.HotRestartModule.newOffHeapHotRestartStore;
import static com.hazelcast.internal.hotrestart.impl.HotRestartModule.newOnHeapHotRestartStore;
import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;

public class MockStoreRegistry implements RamStoreRegistry {

    public final HotRestartStore hrStore;
    public final ConcurrentMap<Long, MockRecordStore> recordStores = new ConcurrentHashMap<Long, MockRecordStore>();
    public final boolean fsyncEnabled;

    private final MemoryManager memMgr;
    private final File backupDir;

    public MockStoreRegistry(HotRestartStoreConfig cfg, MemoryManager memMgr, boolean fsyncEnabled) {
        this.memMgr = memMgr;
        this.fsyncEnabled = fsyncEnabled;
        cfg.setRamStoreRegistry(this);
        this.backupDir = new File(cfg.homeDir().getParentFile(), "backup");
        final HazelcastProperties emptyProperties = new HazelcastProperties(new Properties());
        this.hrStore = memMgr != null
                ? newOffHeapHotRestartStore(cfg, emptyProperties)
                : newOnHeapHotRestartStore(cfg, emptyProperties);
        final RamStoreRestartLoop loop = new RamStoreRestartLoop(1, 1, this, cfg.logger());
        final Throwable[] failure = {null};
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
        assertJoinable(restartIoThread);
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

    public void backup(long backupSeq) {
        final File dir = new File(backupDir, "backup-" + backupSeq);
        dir.mkdirs();
        hrStore.backup(dir);
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
        recordStores.clear();
    }

    MockRecordStore getOrCreateRecordStoreForPrefix(long prefix) {
        MockRecordStore ret = recordStores.get(prefix);
        if (ret == null) {
            ret = memMgr != null
                    ? new MockRecordStoreOffHeap(prefix, memMgr, hrStore, fsyncEnabled)
                    : new MockRecordStoreOnHeap(prefix, hrStore, fsyncEnabled);
            final MockRecordStore existing = recordStores.putIfAbsent(prefix, ret);
            if (existing != null) {
                ret = existing;
            }
        }
        return ret;
    }
}
