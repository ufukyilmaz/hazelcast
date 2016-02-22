package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.DestValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Evacuates source chunks into destination chunks by moving all live records.
 * Dismisses the garbage thus collected and deletes the evacuated source chunks.
 */
final class ValEvacuator {
    public static final String SYSPROP_GC_STUCK_DETECT_THRESHOLD =
            "hazelcast.hotrestart.gc.stuck.detect.threshold";

    private final int stuckDetectionThreshold =
            Integer.getInteger(SYSPROP_GC_STUCK_DETECT_THRESHOLD, 1000 * 1000);
    private final Collection<StableValChunk> srcChunks;
    private final GcLogger logger;
    private final Long2ObjectHashMap<WriteThroughChunk> survivorMap;
    private final TrackerMap recordTrackers;
    private final GcHelper gcHelper;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final StableValChunk firstSrcChunk;
    private long start;
    private DestValChunk survivor;

    private ValEvacuator(Collection<StableValChunk> srcChunks, ChunkManager chunkMgr, MutatorCatchup mc,
                         GcLogger logger, long start
    ) {
        this.srcChunks = srcChunks;
        this.firstSrcChunk = srcChunks.iterator().next();
        this.logger = logger;
        this.survivorMap = chunkMgr.survivors = new Long2ObjectHashMap<WriteThroughChunk>();
        this.gcHelper = chunkMgr.gcHelper;
        this.pfixTombstoMgr = chunkMgr.pfixTombstoMgr;
        this.recordTrackers = chunkMgr.trackers;
        this.mc = mc;
        this.start = start;
    }

    static void evacuate(
            Collection<StableValChunk> srcChunks, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger, long start
    ) {
        new ValEvacuator(srcChunks, chunkMgr, mc, logger, start).evacuate();
    }

    private void evacuate() {
        final SortedBySeqRecordCursor liveRecords = sortedLiveRecords();
        logger.fine("ValueGC preparation took %,d ms ", NANOSECONDS.toMillis(System.nanoTime() - start));
        moveToSurvivor(liveRecords);
        liveRecords.dispose();
        // Apply clear operation to any dangling dest chunks. At the time the clear operation
        // is issued, the highest chunk seq is recorded. Dest chunks created after that time
        // will be missed by the Sweeper.
        for (Chunk c : survivorMap.values()) {
            pfixTombstoMgr.dismissGarbage(c);
        }
    }

    private SortedBySeqRecordCursor sortedLiveRecords() {
        final RecordMap[] recordMaps = new RecordMap[srcChunks.size()];
        mc.catchupNow();
        int i = 0;
        int liveRecordCount = 0;
        for (StableValChunk chunk : srcChunks) {
            recordMaps[i++] = chunk.records;
            liveRecordCount += chunk.liveRecordCount;
        }
        mc.catchupNow();
        return recordMaps[0].sortedBySeqCursor(liveRecordCount, recordMaps, mc);
    }

    private void moveToSurvivor(SortedBySeqRecordCursor sortedCursor) {
        final RecordDataHolder holder = gcHelper.recordDataHolder;
        while (sortedCursor.advance()) {
            applyClearOperation();
            final Record r = sortedCursor.asRecord();
            if (r.isAlive()) {
                holder.clear();
                final KeyHandle kh = sortedCursor.asKeyHandle();
                final RamStore ramStore;
                if ((ramStore = gcHelper.ramStoreRegistry.ramStoreForPrefix(r.keyPrefix(kh))) != null
                        && ramStore.copyEntry(kh, r.payloadSize(), holder)
                ) {
                    // Invariant at this point: r.isAlive() and we have its data. Maintain this invariant by
                    // not catching up with mutator until all metadata are updated. The first catchup can happen
                    // within the writeValueRecord() call (which is called from dest.add()).
                    // By the time dest.add() returns, the record may already be dead.
                    holder.flip();
                    ensureSurvivor();
                    // With moveToChunk() the keyHandle's ownership is transferred to dest.
                    // With dest.add() the record is added to dest. Now its garbage count
                    // will be incremented if the keyHandle receives an update.
                    recordTrackers.get(kh).moveToChunk(survivor.seq);
                    // catches up for each bufferful
                    survivor.add(r, kh, holder);
                    if (survivor.full()) {
                        closeSurvivor();
                    }
                } else {
                    // Our record is alive, but in the RAM store the corresponding entry
                    // was already updated/removed and a retirement event is on its way. We did not move
                    // the record to the dest chunk, so to bring our bookkeeping back in sync we must
                    // keep catching up until we observe the event.
                    if (!catchUpUntilRetired(r, mc)) {
                        final String ramStoreName = ramStore != null ? ramStore.getClass().getSimpleName() : "null";
                        throw new HotRestartException(String.format(
                                "Stuck while waiting for a record to be retired."
                              + " Chunk #%03x, record #%03x, size %,d, RAM store was %s",
                                survivor.seq, r.liveSeq(), r.size(), ramStoreName));
                    }
                }
            }
        }
        if (survivor != null) {
            closeSurvivor();
        }
    }

    private void applyClearOperation() {
        while (firstSrcChunk.needsDismissing()) {
            for (StableValChunk chunk : srcChunks) {
                pfixTombstoMgr.dismissGarbage(chunk);
                mc.catchupNow();
            }
        }
    }

    private void ensureSurvivor() {
        if (survivor != null) {
            return;
        }
        start = System.nanoTime();
        survivor = gcHelper.newDestValChunk(mc);
        survivor.flagForFsyncOnClose(true);
        // make the dest chunk available to chunkMgr.chunk()
        survivorMap.put(survivor.seq, survivor);
    }

    private void closeSurvivor() {
        mc.catchupNow();
        survivor.close();
        mc.catchupNow();
        logger.fine("Wrote chunk #%03x (%,d bytes) in %d ms", survivor.seq, survivor.size(),
                NANOSECONDS.toMillis(System.nanoTime() - start));
        survivor = null;
        mc.catchupNow();
    }

    @SuppressWarnings("checkstyle:emptyblock")
    private boolean catchUpUntilRetired(Record r, MutatorCatchup mc) {
        for (int eventCount = 0;
             eventCount <= stuckDetectionThreshold && r.isAlive();
             eventCount += catchUpSafely(mc, r)
        ) { }
        return !r.isAlive();
    }

    private int catchUpSafely(MutatorCatchup mc, Record r) {
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
}
