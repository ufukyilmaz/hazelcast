package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.ChunkSelection;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Evacuates source chunks into destination chunks by moving all live records.
 * Dismisses the garbage thus collected and deletes the evacuated source chunks.
 */
final class Evacuator {
    private final ChunkSelection selected;
    private final GcLogger logger;
    private final Map<Long, Chunk> destChunkMap;
    private final TrackerMap recordTrackers;
    private final GcHelper gcHelper;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final long start;

    Evacuator(ChunkSelection selected, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger, long start) {
        this.selected = selected;
        this.logger = logger;
        this.destChunkMap = chunkMgr.destChunkMap = new HashMap<Long, Chunk>();
        this.gcHelper = chunkMgr.gcHelper;
        this.pfixTombstoMgr = chunkMgr.pfixTombstoMgr;
        this.recordTrackers = chunkMgr.trackers;
        this.mc = mc;
        this.start = start;
    }

    static List<StableChunk> copyLiveRecords(
            ChunkSelection selected, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger, long start) {
        return new Evacuator(selected, chunkMgr, mc, logger, start).evacuate();
    }

    private List<StableChunk> evacuate() {
        final List<GcRecord> liveRecords = sortedLiveRecords();
        // Sweep the source chunks just before dest chunks are created.
        // This is the last moment where needsDismissing won't need
        // manual propagation to dest chunks.
        for (Chunk c : selected.srcChunks) {
            if (pfixTombstoMgr.dismissGarbage(c)) {
                mc.catchupNow();
            }
        }
        final List<GrowingDestChunk> preparedDestChunks = transferToDest(liveRecords);
        // At this point any further prefix tombstone events will properly
        // raise the needsDismissing flag in all dest chunks, but before it
        // they were not safely propagated. Therefore propagate any needsDismissing
        // flag that a source chunk got to all dest chunks, and dismiss everything
        // that needs to be dismissed.
        propagateDismissing(preparedDestChunks);
        logger.fine("GC preparation took %,d ms ", NANOSECONDS.toMillis(System.nanoTime() - start));
        final List<StableChunk> destChunks = persistDestChunks(preparedDestChunks);
        dismissEvacuatedFiles();
        deleteEmptyDestFiles(destChunks);
        return destChunks;
    }

    private void propagateDismissing(List<GrowingDestChunk> destChunks) {
        if (!propagationNeeded()) {
            return;
        }
        for (Chunk c : destChunks) {
            c.needsDismissing = true;
            pfixTombstoMgr.dismissGarbage(c);
            mc.catchupNow();
        }
    }

    private boolean propagationNeeded() {
        for (Chunk c : selected.srcChunks) {
            if (c.needsDismissing) {
                return true;
            }
        }
        return false;
    }

    private List<GcRecord> sortedLiveRecords() {
        final ArrayList<GcRecord> liveGcRecs = new ArrayList<GcRecord>(selected.liveRecordCount);
        for (StableChunk chunk : selected.srcChunks) {
            for (Cursor cur = chunk.records.cursor(); cur.advance();) {
                if (cur.asRecord().isAlive()) {
                    // Here copies of records are made. The copies will not reflect
                    // new retirements until they are added to the dest chunk in transferToDest.
                    liveGcRecs.add(cur.toGcRecord(chunk.seq));
                }
            }
            mc.catchupNow();
        }
        return sorted(liveGcRecs);
    }

    private List<GrowingDestChunk> transferToDest(List<GcRecord> sortedGcRecords) {
        final List<GrowingDestChunk> destChunks = new ArrayList<GrowingDestChunk>();
        GrowingDestChunk dest = null;
        for (GcRecord gcr : sortedGcRecords) {
            if (dest == null) {
                dest = newDestChunk(destChunks);
            }
            mc.catchupAsNeeded();
            final Tracker tr = recordTrackers.get(gcr.toKeyHandle());
            // This check failing means that the record is now stale. Don't copy it to dest.
            // We cannot use gcr.isAlive as explained in sortedLiveRecords().
            // We might use chunks.get(tr.chunkSeq()).get(gcr.keyHandle).isAlive(),
            // but that would just be needlessly expensive
            if (tr != null && gcr.chunkSeq == tr.chunkSeq()) {
                // With moveToChunk() the keyHandle's ownership is transferred to dest.
                // With dest.add() the GcRecord is added to dest. Now its garbage count
                // will be incremented if the keyHandle receives an update and its isAlive()
                // method will correctly report the status of the record within the dest chunk.
                tr.moveToChunk(dest.seq);
                if (dest.add(gcr)) {
                    dest = null;
                }
            }
        }
        return destChunks;
    }

    private GrowingDestChunk newDestChunk(List<GrowingDestChunk> destChunks) {
        final GrowingDestChunk dest = gcHelper.newDestChunk(pfixTombstoMgr);
        destChunks.add(dest);
        // make the dest chunk available to chunkMgr.chunk()
        destChunkMap.put(dest.seq, dest);
        return dest;
    }

    private List<StableChunk> persistDestChunks(List<GrowingDestChunk> preparedDestChunks) {
        final List<StableChunk> compactedChunks = new ArrayList<StableChunk>();
        for (GrowingDestChunk destChunk : preparedDestChunks) {
            final StableChunk stableChunk = destChunk.flushAndClose(mc, logger);
            compactedChunks.add(stableChunk);
            // This call transfers ownership of records from destChunk to stableChunk.
            // After this point retirements will be addressed at stableChunk.
            destChunkMap.put(stableChunk.seq, stableChunk);
            mc.catchupNow();
        }
        preparedDestChunks.clear();
        return compactedChunks;
    }

    private void dismissEvacuatedFiles() {
        for (StableChunk evacuated : selected.srcChunks) {
            gcHelper.deleteChunkFile(evacuated);
            // All garbage records collected from the source chunk in
            // sortedLiveRecords() and transferToDest() are summarily dismissed by this call
            mc.dismissGarbage(evacuated);
            mc.catchupNow();
        }
    }

    private void deleteEmptyDestFiles(List<StableChunk> destChunks) {
        for (Iterator<StableChunk> iterator = destChunks.iterator(); iterator.hasNext();) {
            final StableChunk c = iterator.next();
            if (c.size() == c.garbage) {
                mc.dismissGarbage(c);
                gcHelper.deleteChunkFile(c);
                iterator.remove();
                mc.catchupNow();
            }
        }
    }

    // gcrs will be random-accessed so insisting on ArrayList
    @SuppressWarnings("checkstyle:illegaltype")
    List<GcRecord> sorted(ArrayList<GcRecord> gcrs) {
        final int size = gcrs.size();
        List<GcRecord> from = gcrs;
        List<GcRecord> to = asList(new GcRecord[size]);
        for (int width = 1; width < size; width *= 2) {
            for (int i = 0; i < size; i += 2 * width) {
                bottomUpMerge(from, i, min(i + width, size), min(i + 2 * width, size), to);
            }
            final List<GcRecord> fromBackup = from;
            from = to;
            to = fromBackup;
        }
        return from;
    }

    private void bottomUpMerge(List<GcRecord> from, int leftStart, int rightStart, int rightEnd, List<GcRecord> to) {
        int currLeft = leftStart;
        int currRight = rightStart;
        for (int j = leftStart; j < rightEnd; j++) {
            final boolean takeLeft = currLeft < rightStart
                    && (currRight >= rightEnd || from.get(currLeft).liveSeq() <= from.get(currRight).liveSeq());
            to.set(j, from.get(takeLeft ? currLeft++ : currRight++));
            mc.catchupAsNeeded();
        }
    }
}
