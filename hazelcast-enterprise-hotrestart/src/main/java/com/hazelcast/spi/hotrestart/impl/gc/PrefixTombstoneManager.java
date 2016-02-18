package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2LongHashMap.LongLongCursor;
import com.hazelcast.util.collection.Long2ObjectHashMap.KeyIterator;
import com.hazelcast.util.collection.LongHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Manages tombstones that inter all entries under a given key prefix.
 * These tombstones are used to implement the Hot Restart store's
 * {@link com.hazelcast.spi.hotrestart.HotRestartStore#clear(long...)} operation.
 */
@SuppressFBWarnings(value = "IS", justification =
        "All accesses of the map referred to by mutatorPrefixTombstones are synchronized."
      + " Setter doesn't need synchronization because it is called before GC thread is started.")
public class PrefixTombstoneManager {
    public static final String NEW_FILE_SUFFIX = ".new";
    public static final int SWEEPING_TIMESLICE_MS = 10;
    private static final long[] EMPTY_LONGS = new long[0];
    long chunkSeqToStartSweep;
    private final GcExecutor gcExec;
    private final GcLogger logger;
    private ChunkManager chunkMgr;
    private Long2LongHashMap mutatorPrefixTombstones;
    private Long2LongHashMap collectorPrefixTombstones;
    private final Long2LongHashMap dismissedActiveChunks;
    private Sweeper sweeper;

    PrefixTombstoneManager(GcExecutor gcExec, GcLogger logger) {
        this.gcExec = gcExec;
        this.logger = logger;
        this.dismissedActiveChunks = new Long2LongHashMap(0);
    }

    void setChunkManager(ChunkManager chunkMgr) {
        this.chunkMgr = chunkMgr;
    }

    void setPrefixTombstones(Long2LongHashMap prefixTombstones) {
        this.mutatorPrefixTombstones = prefixTombstones;
        this.collectorPrefixTombstones = new Long2LongHashMap(prefixTombstones);
    }

    // Called on the mutator thread
    void addPrefixTombstones(long[] prefixes) {
        final GcHelper gcHelper = chunkMgr.gcHelper;
        final Long2LongHashMap tombstoneSnapshot;
        final long currRecordSeq = gcHelper.recordSeq();
        synchronized (this) {
            multiPut(mutatorPrefixTombstones, prefixes, currRecordSeq);
            tombstoneSnapshot = new Long2LongHashMap(mutatorPrefixTombstones);
        }
        gcExec.submit(addedPrefixTombstones(prefixes, currRecordSeq, gcHelper.chunkSeq()));
        persistTombstones(gcHelper, tombstoneSnapshot);
    }

    private Runnable addedPrefixTombstones(final long[] prefixes, final long recordSeq, final long startChunkSeq) {
        return new Runnable() {
            @Override public void run() {
                multiPut(collectorPrefixTombstones, prefixes, recordSeq);
                final Chunk activeChunk = chunkMgr.activeValChunk;
                dismissGarbage(activeChunk, prefixes);
                multiPut(dismissedActiveChunks, prefixes, activeChunk.seq);
                for (StableChunk c : chunkMgr.chunks.values()) {
                    c.needsDismissing(true);
                }
                if (chunkMgr.destChunkMap != null) {
                    for (Chunk c : chunkMgr.destChunkMap.values()) {
                        c.needsDismissing(true);
                    }
                }
                if (sweeper == null) {
                    sweeper = new Sweeper(startChunkSeq);
                    chunkSeqToStartSweep = 0;
                } else {
                    chunkSeqToStartSweep = startChunkSeq;
                }
            }
        };
    }

    @SuppressFBWarnings(value = "QBA",
            justification = "sweptSome = true executes conditionally on sweepNextChunk() returning true."
                          + " sweptSome correctly tells whether some chunk was swept")
    @SuppressWarnings("checkstyle:emptyblock")
    boolean sweepAsNeeded() {
        final long start = System.nanoTime();
        if (sweeper != null) {
            boolean sweptSome = false;
            boolean hadMoreTime = true;
            do { }
            while (sweeper.sweepNextChunk()
                    && (sweptSome = true)
                    && (hadMoreTime = System.nanoTime() - start < MILLISECONDS.toNanos(SWEEPING_TIMESLICE_MS)));
            if (hadMoreTime) {
                sweeper = null;
            }
            return sweptSome;
        }
        if (chunkSeqToStartSweep != 0) {
            sweeper = new Sweeper(chunkSeqToStartSweep);
            chunkSeqToStartSweep = 0;
            sweepAsNeeded();
        }
        sweeper = null;
        return false;
    }

    /**
     * Applies the effects of newly added prefix tombstones to the active chunk.
     * The point is to immediately reset the garbage counts on records in the active chunk so
     * future updates on the same chunk will be distinguished from those that happened before
     * the clear operation. This is required to maintain the correct state of {@code garbageCount}s
     * in the active chunk.
     *
     * @param prefixesToDismiss set of key prefixes for which tombstones were added
     */
    void dismissGarbage(Chunk chunk, long[] prefixesToDismiss) {
        logger.fine("Dismiss garbage in active chunk #%03x", chunk.seq);
        final LongHashSet prefixSetToDismiss = new LongHashSet(prefixesToDismiss, 0);
        for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            final KeyHandle kh = cursor.toKeyHandle();
            final long prefix = r.keyPrefix(kh);
            if (prefixSetToDismiss.contains(prefix)) {
                chunkMgr.dismissPrefixGarbage(chunk, kh, r);
            }
        }
    }

    /**
     * Propagates the effects of all prefix tombstones to the given chunk.
     * Avoids applying a prefix tombstone to the chunk which was active at the time the
     * tombstone was added (that work was already done by {@link #dismissGarbage(Chunk, long[])}).
     *
     * @return true if the chunk needed dismissing.
     */
    public boolean dismissGarbage(Chunk chunk) {
        if (!chunk.needsDismissing()) {
            return false;
        }
        logger.fine("Dismiss garbage in #%03x", chunk.seq);
        for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            final KeyHandle kh = cursor.toKeyHandle();
            final long prefix = r.keyPrefix(kh);
            if (dismissedActiveChunks.get(prefix) != chunk.seq
                    && r.deadOrAliveSeq() <= collectorPrefixTombstones.get(prefix)
            ) {
                chunkMgr.dismissPrefixGarbage(chunk, kh, r);
            }
        }
        chunk.needsDismissing(false);
        return true;
    }

    private void collectGarbageTombstones(Long2LongHashMap garbageTombstones) {
        int collectedCount = 0;
        for (LongLongCursor cursor = garbageTombstones.cursor(); cursor.advance();) {
            if (collectorPrefixTombstones.get(cursor.key()) == cursor.value()) {
                collectorPrefixTombstones.remove(cursor.key());
                dismissedActiveChunks.remove(cursor.key());
                collectedCount++;
            }
        }
        if (collectedCount > 0) {
            logger.info("Collected %,d garbage prefix tombstones", collectedCount);
        }
        synchronized (this) {
            for (LongLongCursor cursor = garbageTombstones.cursor(); cursor.advance();) {
                if (mutatorPrefixTombstones.get(cursor.key()) == cursor.value()) {
                    mutatorPrefixTombstones.remove(cursor.key());
                }
            }
        }
    }

    private void persistTombstones(GcHelper gcHelper, Long2LongHashMap tombstoneSnapshot) {
        final File homeDir = gcHelper.homeDir;
        final File newFile = new File(homeDir, PREFIX_TOMBSTONES_FILENAME + NEW_FILE_SUFFIX);
        FileOutputStream fileOut = null;
        try {
            fileOut = new FileOutputStream(newFile);
            final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOut));
            for (LongLongCursor c = tombstoneSnapshot.cursor(); c.advance();) {
                out.writeLong(c.key());
                out.writeLong(c.value());
            }
            out.flush();
            fileOut.getFD().sync();
            out.close();
            fileOut = null;
            if (!newFile.renameTo(new File(homeDir, PREFIX_TOMBSTONES_FILENAME))) {
                throw new HotRestartException("Failed to rename the prefix tombstones file "
                        + newFile.getAbsolutePath());
            }
            logger.finest("Persisted prefix tombstones %s", tombstoneSnapshot);
        } catch (IOException e) {
            closeIgnoringFailure(fileOut);
            if (!newFile.delete()) {
                logger.severe("Failed to delete " + newFile);
            }
            throw new HotRestartException("IO error while writing prefix tombstones", e);
        } finally {
            closeIgnoringFailure(fileOut);
        }
    }

    private static void multiPut(Long2LongHashMap map, long[] keys, long value) {
        for (long prefix : keys) {
            map.put(prefix, value);
        }
    }

    private final class Sweeper {
        private final Long2LongHashMap garbageTombstones = new Long2LongHashMap(collectorPrefixTombstones);
        private final long lowChunkSeq;
        private final long sweptActiveChunkSeq;
        private long chunkSeq;

        Sweeper(long highChunkSeq) {
            this.chunkSeq = highChunkSeq;
            this.lowChunkSeq = lowChunkSeq();
            final Chunk activeChunk = chunkMgr.activeValChunk;
            if (activeChunk.seq <= highChunkSeq) {
                markLiveTombstones(activeChunk);
                sweptActiveChunkSeq = activeChunk.seq;
            } else {
                sweptActiveChunkSeq = 0;
            }
        }

        /**
         * @return {@code true} if a chunk was swept;
         * {@code false} if there was no more chunk to sweep (current sweep cycle is done).
         */
        boolean sweepNextChunk() {
            final Chunk toSweep = nextChunkToSweep();
            if (toSweep == null) {
                collectGarbageTombstones(garbageTombstones);
                return false;
            }
            dismissGarbage(toSweep);
            if (toSweep.seq != sweptActiveChunkSeq) {
                markLiveTombstones(toSweep);
            }
            return true;
        }

        private void markLiveTombstones(Chunk chunk) {
            for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
                final Record r = cursor.asRecord();
                final KeyHandle kh = cursor.toKeyHandle();
                final long prefix = r.keyPrefix(kh);
                garbageTombstones.remove(prefix);
            }
        }

        private long lowChunkSeq() {
            long lowChunkSeq = Long.MAX_VALUE;
            for (KeyIterator it = chunkMgr.chunks.keySet().iterator(); it.hasNext();) {
                lowChunkSeq = Math.min(lowChunkSeq, it.nextLong());
            }
            return lowChunkSeq;
        }

        private StableValChunk nextChunkToSweep() {
            final Map<Long, StableChunk> chunkMap = chunkMgr.chunks;
            while (chunkSeq >= lowChunkSeq) {
                final StableChunk c = chunkMap.get(chunkSeq--);
                if (c != null && c instanceof StableValChunk) {
                    return (StableValChunk) c;
                }
            }
            return null;
        }
    }
}
