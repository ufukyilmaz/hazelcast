package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.gc.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.spi.hotrestart.impl.gc.Record;
import com.hazelcast.util.collection.Long2LongHashMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Pattern;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.ACTIVE_CHUNK_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BUCKET_DIRNAME_DIGITS;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.CHUNK_FNAME_LENGTH;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.MAX_BUCKET_DIRS;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMBSTONE_VALUE;
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
            return leftSeq == rightSeq ? 0 : leftSeq < rightSeq ? -1 : 1;
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

    private final ByteBuffer headerBuf = ByteBuffer.allocate(Record.HEADER_SIZE);
    private final GcHelper gcHelper;
    private final GcExecutor gcExec;
    private final GcLogger logger;
    private ArrayList<File> chunkFiles;
    private Rebuilder rebuilder;

    /** The following variables are updated by advance() and describe the current chunk file and record. */
    private File chunkFile;
    private long truncationPoint;
    private InputStream in;
    private byte[] key;
    private byte[] value;
    private long seq;
    private long prefix;

    public HotRestarter(GcHelper gcHelper, GcExecutor gcExec) {
        this.gcHelper = gcHelper;
        this.gcExec = gcExec;
        this.logger = gcHelper.logger;
    }

    public void restart(boolean failIfAnyData) {
        final Long2LongHashMap prefixTombstones = restorePrefixTombstones(gcHelper.homeDir);
        logger.finest("Prefix tomstones: ", prefixTombstones);
        this.rebuilder = new Rebuilder(gcExec.chunkMgr, gcHelper.logger, prefixTombstones);
        gcExec.setPrefixTombstones(prefixTombstones);
        final RamStoreRegistry reg = gcHelper.ramStoreRegistry;
        final long highestChunkSeq = init();
        gcHelper.initChunkSeq(highestChunkSeq);
        if (highestChunkSeq == 0) {
            return;
        }
        if (failIfAnyData && advance()) {
            throw new HotRestartException("failIfAnyData == true and there's live data to reload");
        }
        while (advance()) {
            final int recordSize = Record.size(key, value);
            truncationPoint += recordSize;
            final boolean isTombstone = value == TOMBSTONE_VALUE;
            final RamStore ramStore = reg.restartingRamStoreForPrefix(prefix);
            if (ramStore != null) {
                acceptRecord(ramStore, recordSize, isTombstone);
            } else {
                rebuilder.acceptCleared(recordSize);
            }
        }
        rebuilder.done();
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
                (int) f.length() / PREFIX_TOMBSTONE_ENTRY_SIZE, DEFAULT_LOAD_FACTOR, 0L);
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

    /**
     * Initializes the cursor's chunk file queue and opens the file at the queue's head
     * (the youngest chunk file).
     * @return sequence number of the youngest existing chunk
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    private long init() {
        final File[] bucketDirs = gcHelper.homeDir.listFiles(BUCKET_DIRS_ONLY);
        if (bucketDirs == null) {
            throw new HotRestartException("Failed to list directory contents: " + gcHelper.homeDir);
        }
        for (File d : bucketDirs) {
            final File[] chunksInBucket = d.listFiles(CHUNK_FILES_ONLY);
            if (chunksInBucket == null) {
                throw new HotRestartException("Failed to list directory contents: " + d);
            }
            if (chunksInBucket.length == 0) {
                continue;
            }
            if (chunkFiles == null) {
                chunkFiles = new ArrayList<File>(chunksInBucket.length * MAX_BUCKET_DIRS);
            }
            Collections.addAll(chunkFiles, chunksInBucket);
        }
        if (chunkFiles == null) {
            return 0;
        }
        Collections.sort(chunkFiles, BY_SEQ);
        return tryOpenNextChunk() ? rebuilder.currChunkSeq() : 0;
    }

    private boolean advance() {
        while (true) {
            try {
                try {
                    headerBuf.clear();
                    if (readFullyOrNothing(in, headerBuf.array())) {
                        loadRecord();
                        return true;
                    }
                } catch (EOFException e) {
                    if (isActiveChunkFile(chunkFile)) {
                        removeBrokenTrailingRecord();
                    } else {
                        throw e;
                    }
                }
            } catch (IOException e) {
                throw new HotRestartException(e);
            }
            if (!tryOpenNextChunk()) {
                return false;
            }
        }
    }

    private void loadRecord() throws IOException {
        seq = headerBuf.getLong();
        prefix = headerBuf.getLong();
        key = readBlock(headerBuf.getInt());
        final int valueLen = headerBuf.getInt();
        value = (valueLen < 0) ? TOMBSTONE_VALUE : readBlock(valueLen);
    }

    private void acceptRecord(RamStore ramStore, int recordSize, boolean isTombstone) {
        final KeyHandle kh = ramStore.toKeyHandle(key);
        if (rebuilder.accept(prefix, kh, seq, recordSize, isTombstone)) {
            if (isTombstone) {
                ramStore.acceptTombstone(kh, seq);
            } else {
                ramStore.accept(kh, value);
            }
        }
    }

    private byte[] readBlock(int size) throws IOException {
        final byte[] buf = new byte[size];
        readFully(in, buf);
        return buf;
    }

    private void removeBrokenTrailingRecord() {
        try {
            in.close();
            final RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw");
            raf.setLength(truncationPoint);
            raf.getFD().sync();
            raf.close();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private boolean tryOpenNextChunk() {
        try {
            if (in != null) {
                in.close();
                if (isActiveChunkFile(chunkFile)) {
                    removeActiveSuffix(chunkFile);
                }
            }
            final File chunkFile = findNonemptyFile();
            if (chunkFile == null) {
                return false;
            }
            this.chunkFile = chunkFile;
            this.truncationPoint = 0;
            final FileInputStream fileIn = new FileInputStream(chunkFile);
            final boolean compressed = chunkFile.getName().endsWith(COMPRESSED_SUFFIX);
            in = compressed ? gcHelper.compressor.compressedInputStream(fileIn) : new BufferingInputStream(fileIn);
            rebuilder.startNewChunk(seq(chunkFile), compressed);
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @SuppressFBWarnings(value = "RV",
            justification = "Deleting an empty chunk file is non-essential behavior")
    private File findNonemptyFile() {
        if (chunkFiles == null) {
            return null;
        }
        while (chunkFiles.size() > 0) {
            final File f = chunkFiles.remove(chunkFiles.size() - 1);
            if (f.length() != 0) {
                return f;
            }
            f.delete();
        }
        return null;
    }

    private static boolean isActiveChunkFile(File f) {
        return f.getName().endsWith(Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX);
    }

    private static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
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

    private static void readFully(InputStream in, byte[] b) throws IOException {
        if (!readFullyOrNothing(in, b)) {
            throw new EOFException();
        }
    }

    private static void removeActiveSuffix(File activeChunkFile) {
        final String nameNow = activeChunkFile.getName();
        final String nameToBe = nameNow.substring(0, nameNow.length() - ACTIVE_CHUNK_SUFFIX.length());
        if (!activeChunkFile.renameTo(new File(activeChunkFile.getParent(), nameToBe))) {
            throw new HazelcastException("Failed to rename " + nameNow + " to " + nameToBe);
        }
    }

    private static long seq(File f) {
        return parseLong(f.getName().substring(0, CHUNK_FNAME_LENGTH), HEX_RADIX);
    }
}
