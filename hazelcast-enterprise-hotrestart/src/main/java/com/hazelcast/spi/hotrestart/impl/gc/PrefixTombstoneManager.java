package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2LongHashMap.LongLongCursor;
import com.hazelcast.util.collection.LongHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
class PrefixTombstoneManager {
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

    long tombstoneSeqForPrefix(long prefix) {
        return collectorPrefixTombstones.get(prefix);
    }

    private Runnable addedPrefixTombstones(final long[] prefixes, final long recordSeq, final long startChunkSeq) {
        return new Runnable() {
            @Override public void run() {
                multiPut(collectorPrefixTombstones, prefixes, recordSeq);
                final Chunk activeChunk = chunkMgr.activeChunk;
                dismissGarbage(activeChunk, prefixes);
                multiPut(dismissedActiveChunks, prefixes, activeChunk.seq);
                for (StableChunk c : chunkMgr.chunks.values()) {
                    c.needsDismissing = true;
                }
                if (chunkMgr.destChunkMap != null) {
                    for (Chunk c : chunkMgr.destChunkMap.values()) {
                        c.needsDismissing = true;
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

    boolean dismissGarbage(Chunk chunk) {
        return dismissGarbage(chunk, EMPTY_LONGS);
    }

    /**
     * @param prefixesToDismiss Prefixes to dismiss unconditionally. Other prefixes will be dismissed only
     *                          if the chunk seq doesn't match the seq saved in dismissedActiveChunks.
     * @return whether the chunk needed dismissing garbage.
     */
    boolean dismissGarbage(Chunk chunk, long[] prefixesToDismiss) {
        if (prefixesToDismiss.length == 0 && !chunk.needsDismissing) {
            return false;
        }
        logger.fine("Dismiss garbage in #%03x", chunk.seq);
        final LongHashSet prefixSetToDismiss = new LongHashSet(prefixesToDismiss, 0);
        for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            final KeyHandle kh = cursor.toKeyHandle();
            final long prefix = r.keyPrefix(kh);
            if ((prefixSetToDismiss.contains(prefix) || dismissedActiveChunks.get(prefix) != chunk.seq)
                    && r.deadOrAliveSeq() <= collectorPrefixTombstones.get(prefix)) {
                chunkMgr.dismissPrefixGarbage(chunk, kh, r);
            }
        }
        chunk.needsDismissing = false;
        return true;
    }

    private void collectGarbageTombstones(Long2LongHashMap garbageTombstones) {
        boolean collectedSome = false;
        for (LongLongCursor cursor = garbageTombstones.cursor(); cursor.advance();) {
            if (collectorPrefixTombstones.get(cursor.key()) == cursor.value()) {
                collectorPrefixTombstones.remove(cursor.key());
                dismissedActiveChunks.remove(cursor.key());
                collectedSome = true;
            }
        }
        if (collectedSome) {
            logger.finest("Collected some garbage tombstones");
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
        if (gcHelper.ioDisabled()) {
            return;
        }
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
            fileOut.getChannel().force(true);
            out.close();
            fileOut = null;
            if (!newFile.renameTo(new File(homeDir, PREFIX_TOMBSTONES_FILENAME))) {
                throw new HotRestartException("Failed to rename the prefix tombstones file "
                        + newFile.getAbsolutePath());
            }
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
        private long chunkSeq;
        private final long sweptActiveChunkSeq;

        Sweeper(long chunkSeqLimit) {
            this.chunkSeq = chunkSeqLimit;
            final Chunk activeChunk = chunkMgr.activeChunk;
            if (activeChunk.seq <= chunkSeqLimit) {
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

        void markLiveTombstones(Chunk chunk) {
            for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
                final Record r = cursor.asRecord();
                final KeyHandle kh = cursor.toKeyHandle();
                final long prefix = r.keyPrefix(kh);
                garbageTombstones.remove(prefix);
            }
        }

        private Chunk nextChunkToSweep() {
            final Map<Long, StableChunk> chunkMap = chunkMgr.chunks;
            for (; chunkSeq > 0; chunkSeq--) {
                final Chunk c = chunkMap.get(chunkSeq);
                if (c != null) {
                    return c;
                }
            }
            return null;
        }
    }
}
