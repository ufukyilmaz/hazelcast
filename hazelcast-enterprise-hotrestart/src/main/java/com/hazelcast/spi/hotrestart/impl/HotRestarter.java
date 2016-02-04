package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.gc.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.util.collection.Long2LongHashMap;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.ACTIVE_CHUNK_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.TOMB_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.VAL_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BUCKET_DIRNAME_DIGITS;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.CHUNK_FNAME_LENGTH;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static com.hazelcast.util.collection.Long2LongHashMap.DEFAULT_LOAD_FACTOR;
import static java.lang.Long.parseLong;

/**
 * Reads the persistent state and:
 * <ol>
 *     <li>refills the in-memory stores</li>
 *     <li>rebuilds the Hot Restart Store's metadata</li>
 * </ol>
 */
class HotRestarter {
    private static final int HEX_RADIX = 16;
    private static final int PREFIX_TOMBSTONE_ENTRY_SIZE = LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;
    private static final Comparator<File> BY_SEQ = new Comparator<File>() {
        public int compare(File left, File right) {
            final long leftSeq = seq(left);
            final long rightSeq = seq(right);
            return leftSeq < rightSeq ? -1 : leftSeq > rightSeq ? 1 : 0;
        }
    };
    private static final Pattern RX_BUCKET_DIR = Pattern.compile(String.format("[0-9a-f]{%d}", BUCKET_DIRNAME_DIGITS));
    private static final FileFilter BUCKET_DIRS_ONLY = new FileFilter() {
        public boolean accept(File f) {
            return f.isDirectory() && RX_BUCKET_DIR.matcher(f.getName()).matches();
        }
    };
    private static final FileFilter CHUNK_FILES_ONLY = new FileFilter() {
        public boolean accept(File f) {
            return f.isFile() && (f.getName().endsWith(Chunk.FNAME_SUFFIX)
                                  || f.getName().endsWith(Chunk.FNAME_SUFFIX + COMPRESSED_SUFFIX)
                                  || isActiveChunkFile(f));
        }
    };

    private final GcHelper gcHelper;
    private final GcExecutor gcExec;
    private final RamStoreRegistry reg;
    private final Map<Long, SetOfKeyHandle> tombKeys = new HashMap<Long, SetOfKeyHandle>();
    private final GcLogger logger;
    private Rebuilder rebuilder;
    private Long2LongHashMap prefixTombstones;

    // These two are updated on each advancement of the chunk file cursor
    private RamStore ramStore;
    private KeyHandle keyHandle;

    public HotRestarter(GcHelper gcHelper, GcExecutor gcExec) {
        this.gcHelper = gcHelper;
        this.gcExec = gcExec;
        this.reg = gcHelper.ramStoreRegistry;
        this.logger = gcHelper.logger;
    }

    public void restart(boolean failIfAnyData) throws InterruptedException {
        final Long2LongHashMap prefixTombstones = restorePrefixTombstones(gcHelper.homeDir);
        logger.finest("Reloaded prefix tombstones %s", prefixTombstones);
        this.rebuilder = new Rebuilder(gcExec.chunkMgr, gcHelper.logger);
        gcExec.setPrefixTombstones(prefixTombstones);
        this.prefixTombstones = prefixTombstones;
        final ChunkFilesetCursor tombCursor =
                new ChunkFilesetCursor.Tomb(sortedChunkFiles(TOMB_BASEDIR), rebuilder, gcHelper);
        final ChunkFilesetCursor valCursor =
                new ChunkFilesetCursor.Val(sortedChunkFiles(VAL_BASEDIR), rebuilder, gcHelper);
        if (failIfAnyData && (tombCursor.advance() || valCursor.advance())) {
            throw new HotRestartException("failIfAnyData == true and there's data to reload");
        }
        try {
            loadTombstones(tombCursor);
            rebuilder.startValuePhase(tombKeys);
            loadValues(valCursor);
            gcHelper.initChunkSeq(rebuilder.maxChunkSeq());
            rebuilder.done();
            for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
                reg.restartingRamStoreForPrefix(e.getKey()).removeNullEntries(e.getValue());
            }
        } finally {
            for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
                e.getValue().dispose();
            }
        }
    }

    private void loadTombstones(ChunkFilesetCursor tombCursor) throws InterruptedException {
        while (tombCursor.advance()) {
            final ChunkFileRecord rec = tombCursor.currentRecord();
            if (!loadStep1(rec)) {
                continue;
            }
            final long prefix = rec.prefix();
            SetOfKeyHandle sokh = tombKeys.get(prefix);
            if (sokh == null) {
                sokh = gcHelper.newSetOfKeyHandle();
                tombKeys.put(prefix, sokh);
            }
            sokh.add(keyHandle);
            rebuilder.accept(prefix, keyHandle, rec.recordSeq(), rec.size());
        }
    }

    private void loadValues(ChunkFilesetCursor valCursor) throws InterruptedException {
        while (valCursor.advance()) {
            final ChunkFileRecord rec = valCursor.currentRecord();
            if (loadStep1(rec) && rebuilder.accept(rec.prefix(), keyHandle, rec.recordSeq(), rec.size())) {
                ramStore.accept(keyHandle, rec.value());
            }
        }
    }

    private static Long2LongHashMap restorePrefixTombstones(File homeDir) {
        final File f = new File(homeDir, PREFIX_TOMBSTONES_FILENAME);
        if (!f.exists()) {
            return new Long2LongHashMap(0L);
        }
        if (!f.isFile() || !f.canRead()) {
            throw new HotRestartException("Not a regular, readable file: " + f.getAbsolutePath());
        }
        final Long2LongHashMap prefixTombstones = new Long2LongHashMap(
                (int) (f.length() / PREFIX_TOMBSTONE_ENTRY_SIZE), DEFAULT_LOAD_FACTOR, 0L);
        InputStream in = null;
        try {
            in = new BufferingInputStream(new FileInputStream(f));
            final byte[] entryBuf = new byte[PREFIX_TOMBSTONE_ENTRY_SIZE];
            final ByteBuffer byteBuf = ByteBuffer.wrap(entryBuf);
            while (readFullyOrNothing(in, entryBuf)) {
                prefixTombstones.put(byteBuf.getLong(0), byteBuf.getLong(LONG_SIZE_IN_BYTES));
            }
        } catch (IOException e) {
            closeIgnoringFailure(in);
            throw new HotRestartException("Error restoring prefix tombstones", e);
        }
        return prefixTombstones;
    }

    private List<File> sortedChunkFiles(String base) {
        final List<File> files = new ArrayList<File>(128);
        final File[] bucketDirs = new File(gcHelper.homeDir, base).listFiles(BUCKET_DIRS_ONLY);
        if (bucketDirs == null) {
            return files;
        }
        for (File d : bucketDirs) {
            final File[] chunksInBucket = d.listFiles(CHUNK_FILES_ONLY);
            if (chunksInBucket == null) {
                throw new HotRestartException("Failed to list directory contents: " + d);
            }
            if (chunksInBucket.length == 0) {
                continue;
            }
            Collections.addAll(files, chunksInBucket);
        }
        Collections.sort(files, BY_SEQ);
        return files;
    }

    private boolean loadStep1(ChunkFileRecord rec) {
        rebuilder.preAccept(rec.recordSeq(), rec.size());
        if (rec.recordSeq() <= prefixTombstones.get(rec.prefix())) {
            // We are accepting a cleared record (interred by a prefix tombstone)
            rebuilder.acceptCleared(rec.size());
            return false;
        }
        this.ramStore = reg.restartingRamStoreForPrefix(rec.prefix());
        assert ramStore != null : "RAM store registry failed to provide a store for prefix " + rec.prefix();
        this.keyHandle = ramStore.toKeyHandle(rec.key());
        return true;
    }

    static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
        int bytesRead = 0;
        do {
            int count = in.read(b, bytesRead, b.length - bytesRead);
            if (count < 0) {
                if (bytesRead == 0) {
                    return false;
                }
                throw new EOFException();
            }
            bytesRead += count;
        } while (bytesRead < b.length);
        return true;
    }

    static long seq(File f) {
        return parseLong(f.getName().substring(0, CHUNK_FNAME_LENGTH), HEX_RADIX);
    }

    static boolean isActiveChunkFile(File f) {
        return f.getName().endsWith(Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX);
    }
}
