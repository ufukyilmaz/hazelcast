package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Record;
import com.hazelcast.spi.hotrestart.impl.gc.WriteThroughChunk;
import com.hazelcast.util.Preconditions;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMBSTONE_VALUE;

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
    private WriteThroughChunk activeChunk;

    private HotRestartKey preparedTombstoneKey;
    private long preparedTombstoneSeq;

    private HotRestartStoreImpl(String name, ILogger logger, GcHelper gcHelper) {
        this.gcHelper = gcHelper;
        this.name = name;
        this.logger = logger;
        this.gcExec = new GcExecutor(gcHelper, name);
    }

    public static HotRestartStore newOnHeapHotRestartStore(HotRestartStoreConfig cfg) {
        return create(cfg);
    }

    public static HotRestartStore newOffHeapHotRestartStore(HotRestartStoreConfig cfg) {
        Preconditions.checkNotNull(cfg.malloc(), "malloc is null");
        return create(cfg);
    }

    private static HotRestartStore create(HotRestartStoreConfig cfg) {
        Preconditions.checkNotNull(cfg.homeDir(), "homeDir is null");
        Preconditions.checkNotNull(cfg.ramStoreRegistry(), "ramStoreRegistry is null");
        final File canonicalHome = canonicalizeAndValidate(cfg.homeDir());
        return new HotRestartStoreImpl(
                canonicalHome.getAbsoluteFile().getName(),
                cfg.logger(), cfg.malloc() != null ? new GcHelper.OffHeap(cfg) : new GcHelper.OnHeap(cfg));
    }

    private static File canonicalizeAndValidate(File homeDir) {
        try {
            final File canonicalHome = homeDir.getCanonicalFile();
            if (canonicalHome.exists() && !canonicalHome.isDirectory()) {
                throw new HotRestartException("Path refers to a non-directory: " + canonicalHome);
            }
            if (!canonicalHome.exists() && !canonicalHome.mkdirs()) {
                throw new HotRestartException("Could not create the base directory " + canonicalHome);
            }
            return canonicalHome;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }


    @Override public void put(HotRestartKey kh, byte[] value) {
        validateStatus();
        final int size = Record.size(kh.bytes(), value);
        put0(kh, value, gcHelper.nextRecordSeq(size), size, value == TOMBSTONE_VALUE);
    }

    @Override public long removeStep1(HotRestartKey key) {
        validateStatus();
        preparedTombstoneKey = key;
        return preparedTombstoneSeq = gcHelper.nextRecordSeq(Record.size(key.bytes(), TOMBSTONE_VALUE));
    }

    @Override public void removeStep2() {
        final HotRestartKey k = preparedTombstoneKey;
        if (k == null) {
            throw new HotRestartException("removeStep2 not preceded by removeStep1");
        }
        preparedTombstoneKey = null;
        put0(k, TOMBSTONE_VALUE, preparedTombstoneSeq, Record.size(k.bytes(), TOMBSTONE_VALUE), true);
    }

    @Override public void setAutoFsync(boolean fsync) {
        this.autoFsync = fsync;
    }

    @Override public boolean isAutoFsync() {
        return autoFsync;
    }

    @Override public void fsync() {
        activeChunk.fsync();
    }

    @Override public void hotRestart(boolean failIfAnyData) {
        if (activeChunk != null) {
            throw new IllegalStateException("Hot restart already completed");
        }
        new HotRestarter(gcHelper, gcExec).restart(failIfAnyData);
        activeChunk = gcHelper.newActiveChunk();
        gcExec.start();
        gcExec.submitReplaceActiveChunk(null, activeChunk);
        logger.info(String.format("%s reloaded %,d keys; chunk seq %03x",
                name, gcExec.chunkMgr.trackedKeyCount(), activeChunk.seq));
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
        if (activeChunk != null) {
            activeChunk.fsync();
            activeChunk.close();
            if (activeChunk.size() == 0) {
                gcHelper.deleteChunkFile(activeChunk);
            }
            activeChunk = null;
        }
        gcExec.shutdown();
    }

    private long put0(HotRestartKey key, byte[] value, long seq, int size, boolean isTombstone) {
        gcExec.submitRecord(key, seq, size, isTombstone);
        final boolean chunkFull = activeChunk.addStep1(key.prefix(), seq, isTombstone, key.bytes(), value);
        if (chunkFull) {
            activeChunk.close();
            final WriteThroughChunk inactiveChunk = activeChunk;
            activeChunk = gcHelper.newActiveChunk();
            gcExec.submitReplaceActiveChunk(inactiveChunk, activeChunk);
        } else if (autoFsync) {
            activeChunk.fsync();
        }
        return seq;
    }

    private void validateStatus() {
        if (activeChunk == null) {
            throw new HotRestartException("Hot restart not yet complete");
        }
        if (preparedTombstoneKey != null) {
            throw new HotRestartException("removeStep1 not followed by removeStep2");
        }
    }
}
