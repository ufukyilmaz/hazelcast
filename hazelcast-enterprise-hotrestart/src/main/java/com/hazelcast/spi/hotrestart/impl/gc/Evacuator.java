package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.ChunkSelection;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.DestValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.GcRecord;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Evacuates source chunks into destination chunks by moving all live records.
 * Dismisses the garbage thus collected and deletes the evacuated source chunks.
 */
final class Evacuator {
    private final int stuckDetectionThreshold =
            Integer.getInteger("com.hazelcast.spi.hotrestart.gc.stuckDetectThreshold", 1000 * 1000);
    private final ChunkSelection selected;
    private final GcLogger logger;
    private final Long2ObjectHashMap<Chunk> destChunkMap;
    private final TrackerMap recordTrackers;
    private final GcHelper gcHelper;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final StableValChunk firstSrcChunk;
    private long start;
    private DestValChunk dest;

    Evacuator(ChunkSelection selected, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger, long start) {
        this.selected = selected;
        this.firstSrcChunk = selected.srcChunks.iterator().next();
        this.logger = logger;
        this.destChunkMap = chunkMgr.destChunkMap = new Long2ObjectHashMap<Chunk>();
        this.gcHelper = chunkMgr.gcHelper;
        this.pfixTombstoMgr = chunkMgr.pfixTombstoMgr;
        this.recordTrackers = chunkMgr.trackers;
        this.mc = mc;
        this.start = start;
    }

    static void evacuate(
            ChunkSelection selected, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger, long start
    ) {
        new Evacuator(selected, chunkMgr, mc, logger, start).evacuate();
    }

    private void evacuate() {
        final List<GcRecord> liveRecords = sortedLiveRecords();
        logger.fine("ValueGC preparation took %,d ms ", NANOSECONDS.toMillis(System.nanoTime() - start));
        transferToDest(liveRecords);
        // Apply clear operation to any dangling dest chunks. At the time the clear operation
        // is issued, the highest chunk seq is recorded. Dest chunks created after that time
        // will be missed by the Sweeper.
        for (Chunk c : destChunkMap.values()) {
            pfixTombstoMgr.dismissGarbage(c);
        }
        dismissEvacuatedFiles();
    }

    private List<GcRecord> sortedLiveRecords() {
        final ArrayList<GcRecord> liveGcRecs = new ArrayList<GcRecord>(selected.liveRecordCount);
        for (StableValChunk chunk : selected.srcChunks) {
            for (Cursor cur = chunk.records.cursor(); cur.advance();) {
                if (cur.asRecord().isAlive()) {
                    liveGcRecs.add(cur.toGcRecord(chunk));
                }
            }
            mc.catchupNow();
        }
        return sorted(liveGcRecs);
    }

    private void transferToDest(List<GcRecord> sortedGcRecords) {
        final RecordDataHolder holder = gcHelper.recordDataHolder;
        for (GcRecord r : sortedGcRecords) {
            applyClearOperation();
            if (r.isAlive()) {
                holder.clear();
                final KeyHandle kh = r.toKeyHandle();
                final RamStore ramStore;
                if ((ramStore = gcHelper.ramStoreRegistry.ramStoreForPrefix(r.keyPrefix(null))) != null
                        && ramStore.copyEntry(kh, r.payloadSize(), holder)
                ) {
                    // Invariant at this point: r.isAlive() and we have its data. Do not catch up with
                    // mutator until all metadata is updated. The first catchup can happen within the
                    // dest.add() call. By the time dest.add() returns, the record may already be dead.
                    holder.flip();
                    ensureDestChunk();
                    // With moveToChunk() the keyHandle's ownership is transferred to dest.
                    // With dest.add() the GcRecord is added to dest. Now its garbage count
                    // will be incremented if the keyHandle receives an update.
                    recordTrackers.get(kh).moveToChunk(dest.seq);
                    // After this call isAlive() will report the status of the record within the dest chunk.
                    r.movedToDestChunk();
                    // catches up for each bufferful
                    dest.add(r, kh, holder, mc);
                    if (dest.full()) {
                        closeDestChunk();
                    }
                } else {
                    // Our record is alive, but in the in-memory store the corresponding entry
                    // was already updated and a retirement event is on its way. We did not move
                    // the record to the dest chunk, so to bring our bookkeeping back in sync we must
                    // keep catching up until we observe the event.
                    if (!catchUpUntilRetired(r, mc)) {
                        final String ramStoreName = ramStore != null ? ramStore.getClass().getSimpleName() : "null";
                        throw new HotRestartException(String.format(
                                "Stuck while waiting for a record to be retired."
                              + " Chunk #%03x, record #%03x, size %,d, RAM store was %s",
                                dest.seq, r.liveSeq(), r.size(), ramStoreName));
                    }
                }
            }
        }
        if (dest != null) {
            closeDestChunk();
        }
    }

    private void applyClearOperation() {
        while (firstSrcChunk.needsDismissing()) {
            for (StableValChunk chunk : selected.srcChunks) {
                pfixTombstoMgr.dismissGarbage(chunk);
                mc.catchupNow();
            }
        }
    }

    private void ensureDestChunk() {
        if (dest != null) {
            return;
        }
        start = System.nanoTime();
        dest = gcHelper.newGrowingDestValChunk();
        dest.flagForFsyncOnClose(true);
        // make the dest chunk available to chunkMgr.chunk()
        destChunkMap.put(dest.seq, dest);
    }

    private void closeDestChunk() {
        dest.close();
        mc.catchupNow();
        logger.fine("Wrote chunk #%03x (%,d bytes) in %d ms", dest.seq, dest.size(),
                NANOSECONDS.toMillis(System.nanoTime() - start));
        final StableValChunk stable = dest.toStableChunk();
        // Transfers record ownership from the growing to the stable chunk
        destChunkMap.put(stable.seq, stable);
        dest = null;
        mc.catchupNow();
    }

    @SuppressWarnings("checkstyle:emptyblock")
    private boolean catchUpUntilRetired(GcRecord r, MutatorCatchup mc) {
        for (int eventCount = 0;
             eventCount <= stuckDetectionThreshold && r.isAlive();
             eventCount += catchUpSafely(mc, r)
        ) { }
        return !r.isAlive();
    }

    private int catchUpSafely(MutatorCatchup mc, GcRecord r) {
        int eventCount = mc.catchupNow();
        applyClearOperation();
        if (mc.shutdownRequested()) {
            eventCount += mc.catchupNow();
            applyClearOperation();
            if (r.isAlive()) {
                throw new HotRestartException(
                        "Record not available, retirement event not received, shutdown requested");
            }
        }
        return eventCount;
    }

    private void dismissEvacuatedFiles() {
        for (StableValChunk evacuated : selected.srcChunks) {
            gcHelper.deleteChunkFile(evacuated);
            // All garbage records collected from the source chunk in
            // sortedLiveRecords() and transferToDest() are summarily dismissed by this call
            mc.dismissGarbage(evacuated);
            mc.catchupNow();
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
            mc.catchupAsNeeded(1 << 18);
        }
    }
}
