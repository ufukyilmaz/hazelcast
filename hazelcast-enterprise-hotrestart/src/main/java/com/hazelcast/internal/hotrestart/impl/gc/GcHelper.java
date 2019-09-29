package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.SurvivorValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.internal.hotrestart.impl.gc.mem.MmapMalloc;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.SetOfKeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.SetOfKeyHandleOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.internal.hotrestart.impl.gc.tracker.TrackerMapOffHeap;
import com.hazelcast.internal.hotrestart.impl.gc.tracker.TrackerMapOnHeap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.nio.IOUtil.rename;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.ACTIVE_FNAME_SUFFIX;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.SURVIVOR_FNAME_SUFFIX;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOffHeap.newRecordMapOffHeap;
import static com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOffHeap.newTombstoneMapOffHeap;

/**
 * Contains common constants, global counters, static utility methods, and system resource-oriented methods used
 * throughout the Hot Restart module.
 */
public abstract class GcHelper implements Disposable {

    /** Name of the file that contains prefix tombstones */
    public static final String PREFIX_TOMBSTONES_FILENAME = "prefix-tombstones";

    /** A hex digit represents this many bits. */
    public static final int BITS_PER_HEX_DIGIT = 4;

    /**
     * Chunk filename is a zero-padded long in hex notation. This constant equals
     * the max number of hex digits in a long.
     */
    public static final int CHUNK_FNAME_LENGTH = Long.SIZE / BITS_PER_HEX_DIGIT;

    /** The number of hex digits in the name of a bucket dir */
    public static final int BUCKET_DIRNAME_DIGITS = 2;

    /**
     * To optimize file access times, chunk files are distributed across
     * bucket directories. This is the maximum number of such directories.
     * INVARIANT: this number is a power of two.
     */
    public static final int MAX_BUCKET_DIRS = 1 << (BITS_PER_HEX_DIGIT * BUCKET_DIRNAME_DIGITS);

    /** Bitmask used for the operation "modulo MAX_BUCKET_DIRS" */
    public static final int BUCKET_DIR_MASK = MAX_BUCKET_DIRS - 1;

    private static final String BUCKET_DIRNAME_FORMAT = String.format("%%0%dx", BUCKET_DIRNAME_DIGITS);
    private static final String CHUNK_FNAME_FORMAT = String.format("%%0%dx%%s", CHUNK_FNAME_LENGTH);

    /** Hot Restart Store's home directory. */
    final File homeDir;
    final GcLogger logger;

    private final AtomicLong chunkSeq = new AtomicLong();
    private volatile long recordSeq;

    GcHelper(File homeDir, GcLogger logger) {
        this.homeDir = homeDir;
        this.logger = logger;
    }

    /** Creates a new active value chunk file and returns an instance of {@link ActiveValChunk} that wraps it. */
    public ActiveValChunk newActiveValChunk() {
        final long seq = chunkSeq.incrementAndGet();
        return new ActiveValChunk(seq, newRecordMap(false),
                chunkFileOut(chunkFile(VAL_BASEDIR, seq, Chunk.FNAME_SUFFIX + ACTIVE_FNAME_SUFFIX, true), null),
                this);
    }

    /** Creates a new survivor value chunk file and returns an instance of {@link SurvivorValChunk} that wraps it. */
    public final SurvivorValChunk newSurvivorValChunk(MutatorCatchup mc) {
        final long seq = chunkSeq.incrementAndGet();
        return new SurvivorValChunk(seq, newRecordMap(true),
                chunkFileOut(chunkFile(VAL_BASEDIR, seq, Chunk.FNAME_SUFFIX + SURVIVOR_FNAME_SUFFIX, true), mc),
                this);
    }

    /**
     * Creates a new active tombstone chunk file and returns an instance of
     * {@link WriteThroughTombChunk} that wraps it.
     */
    public WriteThroughTombChunk newActiveTombChunk() {
        return newWriteThroughTombChunk(ACTIVE_FNAME_SUFFIX);
    }

    /**
     * Creates a new tombstone chunk file with the given filename suffix and returns an instance of
     * {@link WriteThroughTombChunk} that wraps it.
     */
    final WriteThroughTombChunk newWriteThroughTombChunk(String suffix) {
        final long seq = chunkSeq.incrementAndGet();
        return new WriteThroughTombChunk(seq, suffix, newTombstoneMap(),
                chunkFileOut(chunkFile(TOMB_BASEDIR, seq, Chunk.FNAME_SUFFIX + suffix, true), null),
                this);
    }

    private static ChunkFileOut chunkFileOut(File f, MutatorCatchup mc) {
        try {
            return new ChunkFileOut(f, mc);
        } catch (FileNotFoundException e) {
            throw new HotRestartException(e);
        }
    }

    /** Initializes the chunk sequence counter to the given value. */
    public final void initChunkSeq(long seq) {
        chunkSeq.set(seq);
    }

    /** @return the current value of the chunk sequence counter */
    public final long chunkSeq() {
        return chunkSeq.get();
    }

    /** Initializes the record sequence counter to the given value. */
    final void initRecordSeq(long seq) {
        recordSeq = seq;
    }

    /** @return the current value of the record sequence counter */
    public final long recordSeq() {
        return recordSeq;
    }

    /** Increments the record sequence counter and returns the new value. */
    public final long nextRecordSeq() {
        return ++recordSeq;
    }

    /** Deletes the stable chunk file associated with the given instance of {@link StableChunk}. */
    public void deleteChunkFile(StableChunk chunk) {
        final File toDelete = stableChunkFile(chunk, false);
        deleteFile(toDelete);
    }

    /**
     * Deletes chunk files with the given {@code chunkSeqs}.
     *
     * @param chunkSeqs    the sequences of the chunks to be deleted
     * @param areValChunks if the chunk sequences are for value chunks
     */
    public void deleteChunkFiles(long[] chunkSeqs, boolean areValChunks) {
        final String baseDir = areValChunks ? VAL_BASEDIR : TOMB_BASEDIR;
        for (long seq : chunkSeqs) {
            deleteFile(chunkFile(baseDir, seq, Chunk.FNAME_SUFFIX, false));
        }
    }

    /**
     * Deletes chunk file with the given {@code seq}.
     *
     * @param seq        the sequence of the chunk to be deleted
     * @param isValChunk if the chunk is a value chunk
     */
    public void deleteChunkFile(long seq, boolean isValChunk) {
        final String baseDir = isValChunk ? VAL_BASEDIR : TOMB_BASEDIR;
        deleteFile(chunkFile(baseDir, seq, Chunk.FNAME_SUFFIX, false));
    }

    private static void deleteFile(File toDelete) {
        assert toDelete.exists() : "Attempt to delete non-existent file " + toDelete;
        delete(toDelete);
    }

    /**
     * Changes the filename suffix of a chunk file.
     * @param base name of the chunk's base directory (inside the overal Hot Restart home directory).
     * @param seq chunk's seq
     * @param suffixNow chunk file's current filename suffix
     * @param suffixToBe desired new filename suffix
     */
    // method non-final as a courtesy to Mockito
    public void changeSuffix(String base, long seq, String suffixNow, String suffixToBe) {
        final File nameNow = chunkFile(base, seq, suffixNow, false);
        final File nameToBe = chunkFile(base, seq, suffixToBe, false);
        rename(nameNow, nameToBe);
    }

    /**
     * Returns a {@code File} instance representing the file associated with the given stable chunk.
     * @param chunk the stable chunk
     * @param mkdirs whether to also create any missing ancestor directories of the chunk file
     */
    public final File stableChunkFile(StableChunk chunk, boolean mkdirs) {
        return chunkFile(chunk.base(), chunk.seq, chunk.fnameSuffix(), mkdirs);
    }

    /**
     * Returns a {@code File} instance representing the chunk file described by the parameters.
     * @param base name of the chunk's base directory (inside the overal Hot Restart home directory).
     * @param seq seq of the chunk
     * @param suffix filename suffix
     * @param mkdirs whether to also create any missing ancestor directories of the chunk file
     */
    public File chunkFile(String base, long seq, String suffix, boolean mkdirs) {
        final String bucketDirname = String.format(BUCKET_DIRNAME_FORMAT, seq & BUCKET_DIR_MASK);
        final String chunkFilename = String.format(CHUNK_FNAME_FORMAT, seq, suffix);
        final File bucketDir = new File(new File(homeDir, base), bucketDirname);
        if (mkdirs && !bucketDir.isDirectory() && !bucketDir.mkdirs()) {
            throw new HotRestartException("Cannot create chunk bucket directory " + bucketDir);
        }
        return new File(bucketDir, chunkFilename);
    }

    /**
     * Creates and returns an instance of {@link RecordMap} for a value chunk.
     * @param isForSurvivorValChunk whether the map will be used in a survivor value chunk. In the off-heap case
     *                              this means the auxiliary memory allocater will be used.
     */
    abstract RecordMap newRecordMap(boolean isForSurvivorValChunk);

    /** Creates and returns an instance of {@link RecordMap} for a tombstone chunk. */
    abstract RecordMap newTombstoneMap();

    /** Creates and returns an instance of {@link TrackerMap}. */
    public abstract TrackerMap newTrackerMap();

    /** Creates and returns an instance of {@link SetOfKeyHandle}. */
    public abstract SetOfKeyHandle newSetOfKeyHandle();

    /** The GC helper specialization for on-heap Hot Restart store */
    public static final class OnHeap extends GcHelper {

        @Inject
        private OnHeap(@Name("homeDir") File homeDir, GcLogger logger) {
            super(homeDir, logger);
        }

        @Override
        public RecordMap newRecordMap(boolean ignored) {
            return new RecordMapOnHeap();
        }

        @Override
        RecordMap newTombstoneMap() {
            return newRecordMap(false);
        }

        @Override
        public TrackerMap newTrackerMap() {
            return new TrackerMapOnHeap();
        }

        @Override
        public SetOfKeyHandle newSetOfKeyHandle() {
            return new SetOfKeyHandleOnHeap();
        }

        @Override
        public void dispose() { }
    }

    /** The GC helper specialization for off-heap Hot Restart store */
    public static final class OffHeap extends GcHelper {

        private final MemoryManager ramMgr;
        private final MemoryManager mmapMgr;
        private final MemoryManager mmapMgrWithCompaction;

        @Inject
        private OffHeap(MemoryAllocator malloc, @Name("homeDir") File homeDir, GcLogger logger) {
            super(homeDir, logger);
            this.ramMgr = wrapWithAmem(malloc);
            this.mmapMgr = wrapWithAmem(new MmapMalloc(new File(homeDir, "mmap"), false));
            this.mmapMgrWithCompaction =
                    wrapWithAmem(new MmapMalloc(new File(homeDir, "mmap-with-compaction"), true));
        }

        @Override
        public RecordMap newRecordMap(boolean isForSurvivorValChunk) {
            return isForSurvivorValChunk
                    ? newRecordMapOffHeap(mmapMgr, ramMgr) : newRecordMapOffHeap(ramMgr, null);
        }

        @Override
        public RecordMap newTombstoneMap() {
            return newTombstoneMapOffHeap(ramMgr);
        }

        @Override
        public TrackerMap newTrackerMap() {
            return new TrackerMapOffHeap(ramMgr, mmapMgrWithCompaction.getAllocator());
        }

        @Override
        public SetOfKeyHandle newSetOfKeyHandle() {
            return new SetOfKeyHandleOffHeap(ramMgr);
        }

        @Override
        public void dispose() {
            mmapMgrWithCompaction.dispose();
            mmapMgr.dispose();
        }

        private static MemoryManager wrapWithAmem(MemoryAllocator malloc) {
            return new MemoryManagerBean(malloc, AMEM);
        }
    }
}
