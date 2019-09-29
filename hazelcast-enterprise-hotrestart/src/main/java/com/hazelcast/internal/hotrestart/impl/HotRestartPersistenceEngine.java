package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.impl.di.DiContainer;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;
import com.hazelcast.internal.hotrestart.impl.gc.ChunkManager;
import com.hazelcast.internal.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.PrefixTombstoneManager;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;

import java.io.File;

/**
 * Single-threaded persistence engine behind the {@link ConcurrentHotRestartStore}.
 * This class is not thread-safe. The caller must ensure a <i>happens-before</i>
 * relationship between any two method calls.
 */
public final class HotRestartPersistenceEngine {

    private final Disposable di;
    private final GcExecutor gcExec;
    private final GcHelper gcHelper;
    private final PrefixTombstoneManager pfixTombstoMgr;

    private ActiveChunk activeValChunk;
    private ActiveChunk activeTombChunk;

    @Inject
    HotRestartPersistenceEngine(
            DiContainer di, GcExecutor gcExec, GcHelper gcHelper, PrefixTombstoneManager pfixTombstoMgr) {
        this.di = di;
        this.gcExec = gcExec;
        this.gcHelper = gcHelper;
        this.pfixTombstoMgr = pfixTombstoMgr;
    }

    public void start(ILogger logger, ChunkManager chunkMgr, @Name("storeName") String name) {
        activeValChunk = gcHelper.newActiveValChunk();
        activeTombChunk = gcHelper.newActiveTombChunk();
        gcExec.submitReplaceActiveChunk(null, activeValChunk);
        gcExec.submitReplaceActiveChunk(null, activeTombChunk);
        gcExec.start();
        logger.info(String.format("%s reloaded %,d keys; chunk seq %03x",
                name, chunkMgr.trackedKeyCount(), ((Chunk) activeValChunk).seq));
    }

    void put(HotRestartKey kh, byte[] value, boolean needsFsync) {
        put0(kh, value, needsFsync);
    }

    void remove(HotRestartKey key, boolean needsFsync) {
        put0(key, null, needsFsync);
    }

    void backup(File targetDir) {
        pfixTombstoMgr.backup(targetDir);
        replaceActiveChunk(activeValChunk, false);
        replaceActiveChunk(activeTombChunk, true);
        gcExec.submitBackup(targetDir);
    }

    @SuppressWarnings("checkstyle:innerassignment")
    private void put0(HotRestartKey hrKey, byte[] value, boolean needsFsync) {
        if (activeValChunk == null) {
            throw new HotRestartException("Hot restart not yet complete");
        }
        final int size = Record.size(hrKey.bytes(), value);
        final long seq = gcHelper.nextRecordSeq();
        final boolean isTombstone = value == null;
        ActiveChunk activeChunk = isTombstone ? activeTombChunk : activeValChunk;
        activeChunk.flagForFsyncOnClose(needsFsync);
        gcExec.submitRecord(hrKey, seq, size, isTombstone);
        final boolean full = activeChunk.addStep1(seq, hrKey.prefix(), hrKey.bytes(), value);
        if (full) {
            replaceActiveChunk(activeChunk, isTombstone);
        }
    }

    private void replaceActiveChunk(ActiveChunk activeChunk, boolean isTombstone) {
        final ActiveChunk inactiveChunk = activeChunk;
        if (isTombstone) {
            activeTombChunk = null;
            inactiveChunk.close();
            activeTombChunk = activeChunk = gcHelper.newActiveTombChunk();
        } else {
            activeValChunk = null;
            inactiveChunk.close();
            activeValChunk = activeChunk = gcHelper.newActiveValChunk();
        }
        gcExec.submitReplaceActiveChunk(inactiveChunk, activeChunk);
    }

    /**
     * When this method completes, it is guaranteed that the effects of all preceding
     * calls to {@link #put(HotRestartKey, byte[], boolean) put(key, value, true)},
     * {@link #remove(HotRestartKey, boolean) remove(key, true)}, and {@link #clear(long...) clear(prefixes)}
     * have become persistent. The calls {@code put(key, value, false)} and {@code remove(key, false)}
     * are excluded from this guarantee.
     */
    void fsync() {
        activeValChunk.fsync();
        activeTombChunk.fsync();
    }

    void clear(long... keyPrefixes) {
        if (keyPrefixes.length != 0 && gcHelper.recordSeq() != 0) {
            pfixTombstoMgr.addPrefixTombstones(keyPrefixes);
        }
    }

    void close() {
        closeAndDeleteIfEmpty(activeValChunk);
        activeValChunk = null;
        closeAndDeleteIfEmpty(activeTombChunk);
        activeTombChunk = null;
        gcExec.shutdown();
        di.dispose();
    }

    private void closeAndDeleteIfEmpty(ActiveChunk chunk) {
        if (chunk == null) {
            return;
        }
        chunk.close();
        if (chunk.size() == 0) {
            gcHelper.deleteChunkFile(chunk.toStableChunk());
        }
    }

    final class Put extends RunnableWithStatus {
        final HotRestartKey key;
        final byte[] value;
        final boolean needsFsync;

        Put(HotRestartKey key, byte[] value, boolean needsFsync) {
            super(!needsFsync);
            this.key = key;
            this.value = value;
            this.needsFsync = needsFsync;
        }

        @Override
        public void run() {
            put(key, value, needsFsync);
        }

        @Override
        public String toString() {
            return String.format("Put: needsFsync %b key %s value.length %s", needsFsync, key, value.length);
        }
    }

    final class Remove extends RunnableWithStatus {
        final HotRestartKey key;
        final boolean needsFsync;

        Remove(HotRestartKey key, boolean needsFsync) {
            super(!needsFsync);
            this.key = key;
            this.needsFsync = needsFsync;
        }

        @Override
        public void run() {
            remove(key, needsFsync);
        }

        @Override
        public String toString() {
            return String.format("Put: needsFsync %b key %s", needsFsync, key);
        }
    }

    final class Backup extends RunnableWithStatus {
        private final File targetDir;

        Backup(File targetDir) {
            super(true);
            this.targetDir = targetDir;
        }

        @Override
        public void run() {
            backup(targetDir);
        }
    }


    final class Clear extends RunnableWithStatus {
        final long[] prefixes;

        Clear(long[] prefixes, boolean needsFsync) {
            super(!needsFsync);
            this.prefixes = prefixes;
        }

        @Override
        public void run() {
            clear(prefixes);
        }
    }
}
