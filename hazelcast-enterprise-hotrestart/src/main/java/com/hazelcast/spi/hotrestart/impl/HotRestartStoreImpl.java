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
        checkNotNull(cfg.ramStoreRegistry(), "ramStoreRegistry is null");
        checkNotNull(cfg.metricsRegistry(), "metricsRegistry is null");
        cfg.validateAndCreateHomeDir();
        return new HotRestartStoreImpl(
                cfg, cfg.malloc() != null ? new GcHelper.OffHeap(cfg) : new GcHelper.OnHeap(cfg));
    }

    @Override public void put(HotRestartKey kh, byte[] value, boolean needsFsync) {
        put0(kh, value, needsFsync);
    }

    @Override public void remove(HotRestartKey key, boolean needsFsync) {
        put0(key, null, needsFsync);
    }

    @SuppressWarnings("checkstyle:innerassignment")
    private void put0(HotRestartKey hrKey, byte[] value, boolean needsFsync) {
        validateStatus();
        final int size = Record.size(hrKey.bytes(), value);
        final long seq = gcHelper.nextRecordSeq(size);
        final boolean isTombstone = value == null;
        WriteThroughChunk activeChunk = isTombstone ? activeTombChunk : activeValChunk;
        activeChunk.flagForFsyncOnClose(needsFsync);
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
        }
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
        if (keyPrefixes.length == 0 || gcHelper.recordSeq() == 0) {
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

    private void closeAndDeleteIfEmpty(WriteThroughChunk chunk) {
        if (chunk != null) {
            chunk.close();
            if (chunk.size() == 0) {
                gcHelper.deleteChunkFile(chunk);
            }
        }

    }

    /**
     * Runs the supplied task while GC activity is paused. This method is provided
     * strictly to facilitate testing.
     */
    public void runWhileGcPaused(CatchupRunnable task) {
        gcExec.runWhileGcPaused(task);
    }

    private void validateStatus() {
        if (activeValChunk == null) {
            throw new HotRestartException("Hot restart not yet complete");
        }
    }

    // interfaces that expose some internals exclusively for testing purposes

    public interface CatchupRunnable {
        void run(CatchupTestSupport mc);
    }

    public interface CatchupTestSupport {
        int catchupNow();
    }
}
