package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.util.collection.LongCursor;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import com.hazelcast.internal.util.collection.Long2LongHashMap.LongLongCursor;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap.KeyIterator;
import com.hazelcast.internal.util.collection.LongHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.nio.IOUtil.rename;
import static com.hazelcast.internal.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Manages "prefix tombstones" which inter all entries under a given key prefix.
 * These tombstones are used to implement the Hot Restart store's
 * {@link com.hazelcast.internal.hotrestart.HotRestartStore#clear(boolean, long...)} operation.
 */
@SuppressFBWarnings(value = "IS", justification =
        "All accesses of the map referred to by mutatorPrefixTombstones are synchronized."
                + " Setter doesn't need synchronization because it is called before GC thread is started.")
// class non-final for the sake of Mockito
@SuppressWarnings("checkstyle:finalclass")
public class PrefixTombstoneManager {
    public static final String NEW_FILE_SUFFIX = ".new";
    public static final int SWEEPING_TIMESLICE_MS = 10;

    long chunkSeqToStartSweep;

    private final GcLogger logger;
    private final GcHelper gcHelper;
    private final ConcurrentConveyorSingleQueue<Runnable> conveyor;
    private final ChunkManager chunkMgr;

    private Long2LongHashMap mutatorPrefixTombstones;
    private Long2LongHashMap collectorPrefixTombstones;
    private final Long2LongHashMap dismissedActiveChunks = new Long2LongHashMap(0);
    private Sweeper sweeper;

    @Inject
    PrefixTombstoneManager(
            ChunkManager chunkMgr, GcHelper gcHelper, GcLogger logger,
            @Name("gcConveyor") ConcurrentConveyorSingleQueue<Runnable> conveyor
    ) {
        this.conveyor = conveyor;
        this.chunkMgr = chunkMgr;
        this.gcHelper = gcHelper;
        this.logger = logger;
    }

    /** Returns the maximum observed record seq in the collector prefix tombstone list, regardless of prefix */
    public long maxRecordSeq() {
        if (collectorPrefixTombstones == null) {
            return 0;
        }
        long maxSeq = 0;
        for (LongLongCursor cursor = collectorPrefixTombstones.cursor(); cursor.advance(); ) {
            final long recordSeq = cursor.value();
            if (recordSeq > maxSeq) {
                maxSeq = recordSeq;
            }
        }
        return maxSeq;
    }

    /** Initializes this object with the existing prefix tombstones reloaded from a file. */
    public void setPrefixTombstones(Long2LongHashMap prefixTombstones) {
        this.mutatorPrefixTombstones = prefixTombstones;
        this.collectorPrefixTombstones = new Long2LongHashMap(prefixTombstones);
    }

    /** Adds prefix tombstones for the given key prefixes. Called on the mutator thread. */
    public void addPrefixTombstones(long[] prefixes) {
        final Long2LongHashMap tombstoneSnapshot;
        final long currRecordSeq = gcHelper.recordSeq();
        synchronized (this) {
            multiPut(mutatorPrefixTombstones, prefixes, currRecordSeq);
            tombstoneSnapshot = new Long2LongHashMap(mutatorPrefixTombstones);
        }
        conveyor.submit(addedPrefixTombstones(prefixes, currRecordSeq, gcHelper.chunkSeq()));
        persistTombstones(tombstoneSnapshot);
    }

    private Runnable addedPrefixTombstones(final long[] prefixes, final long recordSeq, final long startChunkSeq) {
        return new Runnable() {
            @Override public void run() {
                multiPut(collectorPrefixTombstones, prefixes, recordSeq);
                final ActiveValChunk activeChunk = chunkMgr.activeValChunk;
                dismissGarbage(activeChunk, prefixes);
                multiPut(dismissedActiveChunks, prefixes, activeChunk.seq);
                for (StableChunk c : chunkMgr.chunks.values()) {
                    c.needsDismissing(true);
                }
                if (chunkMgr.survivors != null) {
                    for (Chunk c : chunkMgr.survivors.values()) {
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

    /**
     * Runs the chunk sweeping process for {@value SWEEPING_TIMESLICE_MS} milliseconds and then returns.
     * Also returns as soon as there are no more chunks to sweep.
     * @return whether any chunk was swept
     */
    @SuppressFBWarnings(value = "QBA",
            justification = "sweptSome = true executes conditionally on sweepNextChunk() returning true."
                          + " sweptSome correctly tells whether some chunk was swept")
    @SuppressWarnings({"checkstyle:emptyblock", "ConstantConditions"})
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

    boolean sweepingInProgress() {
        return sweeper != null || chunkSeqToStartSweep != 0;
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
    void dismissGarbage(ActiveValChunk chunk, long[] prefixesToDismiss) {
        logger.finest("Dismiss garbage in active chunk #%03x", chunk.seq);
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
     * tombstone was added (that work was already done by {@link #dismissGarbage(ActiveValChunk, long[])}).
     *
     * @return true if the chunk needed dismissing.
     */
    public boolean dismissGarbage(Chunk chunk) {
        if (!chunk.needsDismissing()) {
            return false;
        }
        logger.finest("Dismiss garbage in #%03x", chunk.seq);
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

    /**
     * Removes the prefix tombstones contained in the supplied {@code garbageTombstones} map,
     * but only if the record seq for a given tombstone in this map matches record seq in the
     * "official" metadata maps for collector and mutator prefix tombstones.
     */
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
            logger.fine("Collected %,d garbage prefix tombstones", collectedCount);
        }
        synchronized (this) {
            for (LongLongCursor cursor = garbageTombstones.cursor(); cursor.advance();) {
                if (mutatorPrefixTombstones.get(cursor.key()) == cursor.value()) {
                    mutatorPrefixTombstones.remove(cursor.key());
                }
            }
        }
    }

    /**
     * Writes a snapshot of prefix tombstones to a file.
     */
    private void persistTombstones(Long2LongHashMap tombstoneSnapshot) {
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
            rename(newFile, new File(homeDir, PREFIX_TOMBSTONES_FILENAME));
            logger.finestVerbose("Persisted prefix tombstones %s", tombstoneSnapshot);
        } catch (IOException e) {
            IOUtil.closeResource(fileOut);
            if (!newFile.delete()) {
                logger.severe("Failed to delete " + newFile);
            }
            throw new HotRestartException("IO error while writing prefix tombstones", e);
        } finally {
            IOUtil.closeResource(fileOut);
        }
    }

    private static void multiPut(Long2LongHashMap map, long[] keys, long value) {
        for (long prefix : keys) {
            map.put(prefix, value);
        }
    }

    /** Copies the prefix tombstone file to the target directory. */
    public void backup(File targetDir) {
        final File pfixTombstoneFile = new File(gcHelper.homeDir, PREFIX_TOMBSTONES_FILENAME);
        if (pfixTombstoneFile.exists()) {
            IOUtil.copy(pfixTombstoneFile, targetDir);
        }
    }

    /**
     * Manages the incremental sweeping process which visits each value chunk and propagates the effects
     * of all prefix tombstones to it.
     */
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
         * Sweeps a single chunk.
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

        /**
         * Finds all key prefixes present in the given chunk and marks their prefix tombstones as live.
         * Live prefix tombstones must not be garbage-collected.
         */
        private void markLiveTombstones(Chunk chunk) {
            for (Cursor cursor = chunk.records.cursor(); cursor.advance();) {
                final Record r = cursor.asRecord();
                final KeyHandle kh = cursor.toKeyHandle();
                final long prefix = r.keyPrefix(kh);
                garbageTombstones.remove(prefix);
            }
            if (!(chunk instanceof StableValChunk)) {
                return;
            }
            for (LongCursor cursor = ((StableValChunk) chunk).clearedPrefixesFoundAtRestart.cursor(); cursor.advance();) {
                garbageTombstones.remove(cursor.value());
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
