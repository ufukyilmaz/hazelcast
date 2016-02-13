package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.DestValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.record.SetOfKeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.gc.record.SetOfKeyHandleOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMapOffHeap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMapOnHeap;
import com.hazelcast.spi.hotrestart.impl.io.BufferedOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.ACTIVE_CHUNK_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.DEST_FNAME_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOffHeap.newRecordMapOffHeap;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOffHeap.newTombstoneMapOffHeap;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains common services needed across the hot restart codebase. Passed
 * around a lot due to the lack of proper DI support in current HZ.
 */
public abstract class GcHelper {
    /** Name of the file that contains prefix tombstones */
    public static final String PREFIX_TOMBSTONES_FILENAME = "prefix-tombstones";

    /** A hex digit represents this many bits. */
    public static final int BITS_PER_HEX_DIGIT = 4;

    /** Chunk filename is a zero-padded long in hex notation. This constant equals
     * the max number of hex digits in a long. */
    public static final int CHUNK_FNAME_LENGTH = Long.SIZE / BITS_PER_HEX_DIGIT;

    /** The number of hex digits in the name of a bucket dir */
    public static final int BUCKET_DIRNAME_DIGITS = 2;

    /** To optimize file access times, chunk files are distributed across
     * bucket directories. This is the maximum number of such directories.
     * INVARIANT: this number is a power of two. */
    public static final int MAX_BUCKET_DIRS = 1 << (BITS_PER_HEX_DIGIT * BUCKET_DIRNAME_DIGITS);

    /** Bitmask used for the operation "modulo MAX_BUCKET_DIRS" */
    public static final int BUCKET_DIR_MASK = MAX_BUCKET_DIRS - 1;

    private static final String BUCKET_DIRNAME_FORMAT = String.format("%%0%dx", BUCKET_DIRNAME_DIGITS);
    private static final String CHUNK_FNAME_FORMAT = String.format("%%0%dx%%s", CHUNK_FNAME_LENGTH);

    /** Hot Restart Store's home directory. */
    public final File homeDir;

    /** In-memory store registry used by this Hot Restart Store. */
    public final RamStoreRegistry ramStoreRegistry;

    /** Record Data Handler singleton, used by the GC process to
     * transfer data from the in-memory store. */
    public final RecordDataHolder recordDataHolder = new RecordDataHolder();

    public final GcLogger logger;

    private final AtomicLong chunkSeq = new AtomicLong();

    private volatile long recordSeq;

    public GcHelper(HotRestartStoreConfig cfg) {
        this.homeDir = cfg.homeDir();
        checkNotNull(cfg.logger(), "Logger is null");
        this.logger = new GcLogger(cfg.logger());
        logger.info("homeDir " + homeDir);
        this.ramStoreRegistry = cfg.ramStoreRegistry();
    }

    @SuppressWarnings("checkstyle:emptyblock")
    public static void closeIgnoringFailure(Closeable toClose) {
        if (toClose != null) {
            try {
                toClose.close();
            } catch (IOException ignored) {
            }
        }
    }

    /** @return whether file I/O is disabled. Should return true only in testing. */
    public static boolean ioDisabled() {
        return false;
    }

    public final ActiveValChunk newActiveValChunk() {
        final long seq = chunkSeq.incrementAndGet();
        return new ActiveValChunk(seq, ACTIVE_CHUNK_SUFFIX, newRecordMap(),
                createFileOutputStream(chunkFile(VAL_BASEDIR, seq, Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX, true)),
                this);
    }

    public final DestValChunk newDestValChunk() {
        final long seq = chunkSeq.incrementAndGet();
        return new DestValChunk(seq, newRecordMap(),
                createFileOutputStream(chunkFile(VAL_BASEDIR, seq, Chunk.FNAME_SUFFIX + DEST_FNAME_SUFFIX, true)),
                this);
    }

    public final WriteThroughTombChunk newActiveTombChunk() {
        return newWriteThroughTombChunk(ACTIVE_CHUNK_SUFFIX);
    }

    final WriteThroughTombChunk newWriteThroughTombChunk(String suffix) {
        final long seq = chunkSeq.incrementAndGet();
        return new WriteThroughTombChunk(seq, suffix, newTombstoneMap(),
                createFileOutputStream(chunkFile(TOMB_BASEDIR, seq, Chunk.FNAME_SUFFIX + suffix, true)),
                this);
    }

    public final void initChunkSeq(long seq) {
        chunkSeq.set(seq);
    }

    public final long chunkSeq() {
        return chunkSeq.get();
    }

    final void initRecordSeq(long seq) {
        recordSeq = seq;
    }

    public final long recordSeq() {
        return recordSeq;
    }

    public final long nextRecordSeq() {
        return ++recordSeq;
    }

    public final void deleteChunkFile(Chunk chunk) {
        final File toDelete = chunkFile(chunk, false);
        assert toDelete.exists() : "Attempt to delete non-existent file " + toDelete;
        delete(toDelete);
    }

    public final void changeSuffix(String base, long seq, String suffixNow, String suffixToBe) {
        final File nameNow = chunkFile(base, seq, suffixNow, false);
        final File nameToBe = chunkFile(base, seq, suffixToBe, false);
        if (!nameNow.renameTo(nameToBe)) {
            throw new HazelcastException("Failed to rename " + nameNow + " to " + nameToBe);
        }
    }

    public final File chunkFile(Chunk chunk, boolean mkdirs) {
        return chunkFile(chunk.base(), chunk.seq, chunk.fnameSuffix(), mkdirs);
    }

    public final File chunkFile(String base, long seq, String suffix, boolean mkdirs) {
        final String bucketDirname = String.format(BUCKET_DIRNAME_FORMAT, seq & BUCKET_DIR_MASK);
        final String chunkFilename = String.format(CHUNK_FNAME_FORMAT, seq, suffix);
        final File bucketDir = new File(new File(homeDir, base), bucketDirname);
        if (mkdirs && !bucketDir.isDirectory() && !bucketDir.mkdirs()) {
            throw new HotRestartException("Cannot create chunk bucket directory " + bucketDir);
        }
        return new File(bucketDir, chunkFilename);
    }

    abstract RecordMap newRecordMap();

    abstract RecordMap newTombstoneMap();

    public abstract TrackerMap newTrackerMap();

    public abstract SetOfKeyHandle newSetOfKeyHandle();

    /** The GC helper specialization for on-heap Hot Restart store */
    public static class OnHeap extends GcHelper {

        public OnHeap(HotRestartStoreConfig cfg) {
            super(cfg);
        }

        @Override public RecordMap newRecordMap() {
            return new RecordMapOnHeap();
        }

        @Override RecordMap newTombstoneMap() {
            return newRecordMap();
        }

        @Override public TrackerMap newTrackerMap() {
            return new TrackerMapOnHeap();
        }

        @Override public SetOfKeyHandle newSetOfKeyHandle() {
            return new SetOfKeyHandleOnHeap();
        }
    }

    /** The GC helper specialization for off-heap Hot Restart store */
    public static class OffHeap extends GcHelper {

        private final MemoryAllocator malloc;
        public OffHeap(HotRestartStoreConfig cfg) {
            super(cfg);
            this.malloc = cfg.malloc();
        }

        @Override public RecordMap newRecordMap() {
            return newRecordMapOffHeap(malloc);
        }

        @Override public RecordMap newTombstoneMap() {
            return newTombstoneMapOffHeap(malloc);
        }

        @Override public TrackerMap newTrackerMap() {
            return new TrackerMapOffHeap(malloc);
        }

        @Override public SetOfKeyHandle newSetOfKeyHandle() {
            return new SetOfKeyHandleOffHeap(malloc);
        }
    }

    public static FileOutputStream createFileOutputStream(File f) {
        try {
            return new FileOutputStream(f);
        } catch (FileNotFoundException e) {
            throw new HotRestartException(e);
        }
    }

    public static FileInputStream createFileInputStream(File f) {
        try {
            return new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new HotRestartException(e);
        }
    }

    public static OutputStream bufferedOutputStream(FileOutputStream out) {
        return out == null ? nullOutputStream() : new BufferedOutputStream(out);
    }

    static OutputStream nullOutputStream() {
        return new OutputStream() {
            @Override public void write(int i) throws IOException { }
        };
    }
}
