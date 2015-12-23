package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Record;
import com.hazelcast.spi.hotrestart.impl.gc.WriteThroughChunk;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * This class is not thread-safe. The caller must ensure a
 * <i>happens-before</i> relationship between any two operations on this object.
 */
public final class HotRestartStoreImpl implements HotRestartStore {
    private boolean autoFsync;
    private final String name;
    private final ILogger logger;
    private final GcExecutor gcExec;
    private final GcHelper gcHelper;
    private WriteThroughChunk activeValChunk;
    private WriteThroughChunk activeTombChunk;

    private HotRestartStoreImpl(HotRestartStoreConfig cfg, GcHelper gcHelper) {
        this.gcHelper = gcHelper;
        this.name = cfg.storeName();
        this.logger = cfg.logger();
        this.gcExec = new GcExecutor(cfg, gcHelper);
    }

    public static HotRestartStore newOnHeapHotRestartStore(HotRestartStoreConfig cfg) {
        return create(cfg);
    }

    public static HotRestartStore newOffHeapHotRestartStore(HotRestartStoreConfig cfg) {
        checkNotNull(cfg.malloc(), "malloc is null");
        return create(cfg);
    }

    private static HotRestartStore create(HotRestartStoreConfig cfg) {
        checkNotNull(cfg.homeDir(), "homeDir is null");
        checkNotNull(cfg.ramStoreRegistry(), "ramStoreRegistry is null");
        cfg.validateAndCreateHomeDir();
        return new HotRestartStoreImpl(
                cfg, cfg.malloc() != null ? new GcHelper.OffHeap(cfg) : new GcHelper.OnHeap(cfg));
    }

    @Override public void put(HotRestartKey kh, byte[] value) {
        put0(kh, value);
    }

    @Override public void remove(HotRestartKey key) {
        put0(key, null);
    }

    @SuppressWarnings("checkstyle:innerassignment")
    private void put0(HotRestartKey hrKey, byte[] value) {
        validateStatus();
        final int size = Record.size(hrKey.bytes(), value);
        final long seq = gcHelper.nextRecordSeq(size);
        final boolean isTombstone = value == null;
        WriteThroughChunk activeChunk = isTombstone ? activeTombChunk : activeValChunk;
        gcExec.submitRecord(hrKey, seq, size, isTombstone);
        final boolean full = activeChunk.addStep1(hrKey.prefix(), seq, hrKey.bytes(), value);
        if (full) {
            activeChunk.close();
            final WriteThroughChunk inactiveChunk = activeChunk;
            if (isTombstone) {
                activeChunk = activeTombChunk = gcHelper.newActiveTombChunk();
            } else {
                activeChunk = activeValChunk = gcHelper.newActiveValChunk();
            }
            gcExec.submitReplaceActiveChunk(inactiveChunk, activeChunk);
        } else if (autoFsync) {
            activeChunk.fsync();
        }
    }

    @Override public void setAutoFsync(boolean fsync) {
        this.autoFsync = fsync;
    }

    @Override public boolean isAutoFsync() {
        return autoFsync;
    }

    @Override public void fsync() {
        activeValChunk.fsync();
        activeTombChunk.fsync();
    }

    @Override public void hotRestart(boolean failIfAnyData) throws InterruptedException {
        if (activeValChunk != null) {
            throw new IllegalStateException("Hot restart already completed");
        }
        new HotRestarter(gcHelper, gcExec).restart(failIfAnyData);
        activeValChunk = gcHelper.newActiveValChunk();
        activeTombChunk = gcHelper.newActiveTombChunk();
        gcExec.submitReplaceActiveChunk(null, activeValChunk);
        gcExec.submitReplaceActiveChunk(null, activeTombChunk);
        gcExec.start();
        logger.info(String.format("%s reloaded %,d keys; chunk seq %03x",
                name, gcExec.chunkMgr.trackedKeyCount(), activeValChunk.seq));
    }

    @Override public boolean isEmpty() {
        return gcExec.chunkMgr.trackedKeyCount() == 0;
    }

    @Override public void clear(long... keyPrefixes) {
        if (keyPrefixes.length == 0) {
            return;
        }
        gcExec.addPrefixTombstones(keyPrefixes);
    }

    @Override public String name() {
        return name;
    }

    @Override public void close() {
        closeAndDeleteIfEmpty(activeValChunk);
        activeValChunk = null;
        closeAndDeleteIfEmpty(activeTombChunk);
        activeTombChunk = null;
        gcExec.shutdown();
    }

    private void closeAndDeleteIfEmpty(WriteThroughChunk cuhnk) {
        if (cuhnk != null) {
            cuhnk.close();
            if (cuhnk.size() == 0) {
                gcHelper.deleteChunkFile(cuhnk);
            }
        }

    }

    /**
     * Runs the supplied task while GC activity is paused. This method is provided
     * strictly to facilitate testing.
     */
    public void runWhileGcPaused(Runnable task) {
        gcExec.runWhileGcPaused(task);
    }

    private void validateStatus() {
        if (activeValChunk == null) {
            throw new HotRestartException("Hot restart not yet complete");
        }
    }
}
