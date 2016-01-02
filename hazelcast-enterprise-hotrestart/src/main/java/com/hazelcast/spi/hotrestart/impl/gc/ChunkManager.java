package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.ChunkSelection;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import com.hazelcast.util.counters.Counter;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.selectChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.Evacuator.copyLiveRecords;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages chunk files and contains top-level code of the GC algorithm.
 */
public final class ChunkManager {
    /** Minimum chunk file size to compress. */
    private static final int MIN_SIZE_TO_COMPRESS = 64 * 1024;
    /** Batch size while releasing tombstones. Between batches we catch up and Thread.yield */
    private static final int TOMBSTONE_RELEASING_BATCH_SIZE = 4 * 1024;
    private static final double UNIT_PERCENTAGE = 100.0;
    @Probe(level = MANDATORY) final Counter valOccupancy = newSwCounter();
    @Probe(level = MANDATORY) final Counter valGarbage = newSwCounter();
    @Probe(level = MANDATORY) final Counter tombOccupancy = newSwCounter();
    @Probe(level = MANDATORY) final Counter tombGarbage = newSwCounter();
    final Long2ObjectHashMap<StableChunk> chunks = new Long2ObjectHashMap<StableChunk>();
    final TrackerMap trackers;
    final GcHelper gcHelper;
    final PrefixTombstoneManager pfixTombstoMgr;
    // temporary storage during GC
    Long2ObjectHashMap<Chunk> destChunkMap;
    // temporary storage during GC
    WriteThroughValChunk activeValChunk;
    WriteThroughTombChunk activeTombChunk;
    private final List<StableTombChunk> tombChunksToDelete = new ArrayList<StableTombChunk>();
    private final GcLogger logger;

    public ChunkManager(HotRestartStoreConfig cfg, GcHelper gcHelper, PrefixTombstoneManager pfixTombstoMgr) {
        this.gcHelper = gcHelper;
        this.logger = gcHelper.logger;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.trackers = gcHelper.newTrackerMap();
        final MetricsRegistry metrics = cfg.metricsRegistry();
        final String metricsPrefix = "hot-restart." + cfg.storeName();
        metrics.scanAndRegister(this, metricsPrefix);
        metrics.scanAndRegister(trackers, metricsPrefix);
    }

    public long trackedKeyCount() {
        return trackers.size();
    }

    public void close() {
        activeValChunk.dispose();
        activeTombChunk.dispose();
        for (Chunk c : chunks.values()) {
            c.dispose();
        }
        trackers.dispose();
    }

    /** Accounts for the active value chunk having been inactivated
     * and replaced with a new one. */
    class ReplaceActiveChunk implements Runnable {
        private final WriteThroughChunk fresh;
        private final WriteThroughChunk closed;
        private final boolean isTombChunk;

        public ReplaceActiveChunk(WriteThroughChunk fresh, WriteThroughChunk closed) {
            this.fresh = fresh;
            this.closed = closed;
            this.isTombChunk = fresh instanceof WriteThroughTombChunk;
        }

        @Override public void run() {
            if (isTombChunk) {
                activeTombChunk = (WriteThroughTombChunk) fresh;
            } else {
                activeValChunk = (WriteThroughValChunk) fresh;
            }
            if (closed == null) {
                return;
            }
            final StableChunk stable = closed.toStableChunk();
            (isTombChunk ? tombOccupancy : valOccupancy).inc(stable.size());
            (isTombChunk ? tombGarbage : valGarbage).inc(stable.garbage);
            chunks.put(stable.seq, stable);
        }
    }

    /** Accounts for a new record having been written to a chunk file. */
    class AddRecord implements Runnable {
        private final long prefix;
        private final KeyHandle keyHandle;
        private final long seq;
        private final int size;
        private final boolean isTombstone;

        public AddRecord(HotRestartKey hrKey, long seq, int size, boolean isTombstone) {
            this.prefix = hrKey.prefix();
            this.keyHandle = hrKey.handle();
            this.seq = seq;
            this.size = size;
            this.isTombstone = isTombstone;
        }

        @Override public void run() {
            final GrowingChunk activeChunk = isTombstone ? activeTombChunk : activeValChunk;
            final Tracker tr = trackers.putIfAbsent(keyHandle, activeChunk.seq, false);
            if (tr != null) {
                if (tr.isAlive()) {
                    final Chunk chunk = chunk(tr.chunkSeq());
                    retire(chunk, keyHandle, chunk.records.get(keyHandle));
                }
                tr.newLiveRecord(activeChunk.seq, isTombstone, trackers, false);
            } else {
                assert !isTombstone : "Attempted to add a tombstone for non-existing key";
            }
            activeChunk.addStep2(prefix, keyHandle, seq, size, isTombstone);
        }

        @Override public String toString() {
            return "(" + keyHandle + ',' + seq + ',' + size + ',' + isTombstone + ')';
        }
    }

    void retire(Chunk chunk, KeyHandle kh, Record r) {
        adjustGlobalGarbage(chunk, r);
        chunk.retire(kh, r);
        submitForDeletionAsNeeded(chunk);
    }

    void retireTombstone(KeyHandle kh, long chunkSeq) {
        final Chunk chunk = chunk(chunkSeq);
        final Record r = chunk.records.get(kh);
        retire(chunk, kh, r);
        trackers.removeLiveTombstone(kh);
    }

    void dismissGarbage(Chunk c) {
        for (Cursor cursor = c.records.cursor(); cursor.advance();) {
            final KeyHandle kh = cursor.toKeyHandle();
            final Record r = cursor.asRecord();
            final Tracker tr = trackers.get(kh);
            if (tr != null) {
                dismissChunkGarbageForKey(kh, r, tr);
            } else {
                assert r.garbageCount() == 0
                        : "Inconsistent zero global garbage count and local count " + r.garbageCount();
            }
        }
        c.garbage = 0;
    }

    private void dismissChunkGarbageForKey(KeyHandle kh, Record r, Tracker tr) {
        tr.decrementGarbageCount(r.garbageCount());
        r.setGarbageCount(0);
        final long newCount = tr.garbageCount();
        if (newCount == 0) {
            if (tr.isTombstone()) {
                retireTombstone(kh, tr.chunkSeq());
            } else {
                trackers.removeIfDead(kh, tr);
            }
        } else {
            assert newCount >= 0
                    : String.format("Garbage count for %s (live in #%03x) went below zero: %d - %d = %d",
                        kh, tr.chunkSeq(), tr.garbageCount(), r.garbageCount(), newCount);
        }
    }

    void dismissGarbageRecord(Chunk c, KeyHandle kh, GcRecord r) {
        assert !r.isTombstone() : "Attempted to dismiss garbage for tombstone record";
        final int localCount = r.decrementGarbageCount();
        assert localCount >= 0
                : String.format("Record.garbageCount went below zero in chunk #%03x, record #%d",
                    c.seq, r.rawSeqValue());
        final Tracker tr = trackers.get(kh);
        final boolean globalCountIsZero = tr.decrementGarbageCount(1);
        if (globalCountIsZero) {
            assert localCount == 0 : "Inconsistent zero global garbage count and local count " + localCount;
            if (tr.isTombstone()) {
                retireTombstone(kh, tr.chunkSeq());
            } else {
                trackers.removeIfDead(kh, tr);
            }
        }
    }

    void dismissPrefixGarbage(Chunk chunk, KeyHandle kh, Record r) {
        final Tracker tr = trackers.get(kh);
        if (r.isAlive()) {
            adjustGlobalGarbage(chunk, r);
            chunk.retire(kh, r, false);
            submitForDeletionAsNeeded(chunk);
            tr.retire(trackers);
        }
        if (tr != null) {
            tr.decrementGarbageCount(r.garbageCount());
            if (tr.garbageCount() == 0) {
                trackers.removeIfDead(kh, tr);
            }
        } else {
            assert r.garbageCount() == 0 : "Inconsistent global zero garbage count vs. local " + r.garbageCount();
        }
        r.setGarbageCount(0);
    }

    void submitForDeletionAsNeeded(Chunk chunk) {
        if (chunk.liveRecordCount == 0 && chunk instanceof StableTombChunk) {
            tombChunksToDelete.add((StableTombChunk) chunk);
        }
    }

    boolean deleteGarbageTombChunks(MutatorCatchup mc) {
        if (tombChunksToDelete.isEmpty()) {
            return false;
        }
        final int deleteCount = tombChunksToDelete.size();
        logger.info("Deleting %d tombstone chunks", deleteCount);
        final StableTombChunk[] toDelete = tombChunksToDelete.toArray(new StableTombChunk[deleteCount]);
        tombChunksToDelete.clear();
        for (StableTombChunk chunk : toDelete) {
            tombOccupancy.inc(-chunk.size());
            tombGarbage.inc(-chunk.size());
            disposeAndRemove(chunk);
        }
        for (StableTombChunk chunk : toDelete) {
            if (mc != null) {
                mc.catchupNow();
            }
            gcHelper.deleteChunkFile(chunk);
        }
        return true;
    }

    private void adjustGlobalGarbage(Chunk chunk, Record r) {
        if (chunk != activeValChunk && chunk != activeTombChunk) {
            // Garbage in the active chunk will be accounted for upon deactivation
            (r.isTombstone() ? tombGarbage : valGarbage).inc(r.size());
        }
    }

    private Chunk chunk(long chunkSeq) {
        if (chunkSeq == activeValChunk.seq) {
            return activeValChunk;
        } else if (chunkSeq == activeTombChunk.seq) {
            return activeTombChunk;
        } else {
            final StableChunk c = chunks.get(chunkSeq);
            final Chunk chunk = c != null ? c : destChunkMap != null ? destChunkMap.get(chunkSeq) : null;
            assert chunk != null : String.format("Failed to fetch the chunk #%03x", chunkSeq);
            return chunk;
        }
    }

    GcParams gcParams() {
        return GcParams.gcParams(valGarbage.get(), valOccupancy.get(), gcHelper.recordSeq());
    }

    @SuppressWarnings("checkstyle:innerassignment")
    boolean gc(GcParams gcp, GcExecutor.MutatorCatchup mc) {
        final long start = System.nanoTime();
        final ChunkSelection selected;
        if (gcp == GcParams.ZERO || (selected = selectChunksToCollect(
                chunks.values(), gcp, pfixTombstoMgr, mc, logger)).srcChunks.isEmpty()) {
            return false;
        }
        final long garbageBeforeGc = valGarbage.get();
        final long liveBeforeGc = valOccupancy.get() - garbageBeforeGc;
        logger.fine("Start GC: g/l %2.0f%% (%,d/%,d); costGoal %,d; benefitGoal %,d; min b/c %,.2f",
                UNIT_PERCENTAGE * garbageBeforeGc / liveBeforeGc, garbageBeforeGc, liveBeforeGc,
                gcp.costGoal, gcp.benefitGoal, gcp.minBenefitToCost);
        if (gcp.forceGc) {
            logger.info("Forcing GC due to ratio %.2f", (float) garbageBeforeGc / liveBeforeGc);
        }
        final List<StableValChunk> destChunks = copyLiveRecords(selected, this, mc, logger, start);
        long sizeBefore = 0;
        for (StableValChunk src : selected.srcChunks) {
            sizeBefore += src.size();
            disposeAndRemove(src);
        }
        long sizeAfter = 0;
        for (StableValChunk dest : destChunks) {
            chunks.put(dest.seq, dest);
            sizeAfter += dest.size();
        }
        this.destChunkMap = null;
        final long reclaimed = sizeBefore - sizeAfter;
        final long garbageAfterGc = valGarbage.inc(-reclaimed);
        final long liveAfterGc = valOccupancy.inc(-reclaimed) - garbageAfterGc;
        logger.info("%nDone GC: took %,3d ms; b/c %3.1f g/l %2.0f%% benefit %,d cost %,d garbage %,d live %,d"
                + " tombGarbage %,d tombLive %,d",
                NANOSECONDS.toMillis(System.nanoTime() - start),
                (double) reclaimed / sizeAfter, UNIT_PERCENTAGE * garbageAfterGc / liveAfterGc,
                reclaimed, sizeAfter, garbageAfterGc, liveAfterGc, tombGarbage.get(), tombOccupancy.get());
        assert garbageAfterGc >= 0 : String.format("Garbage went below zero: %,d", garbageAfterGc);
        assert liveAfterGc >= 0 : String.format("Live went below zero: %,d", liveAfterGc);
        return true;
    }

    private void disposeAndRemove(StableChunk chunk) {
        chunk.dispose();
        chunks.remove(chunk.seq);
    }



    boolean compressAllChunks(MutatorCatchup mc) {
        boolean didCatchUp = false;
        for (StableChunk c : chunks.values()) {
            if (!c.compressed) {
                didCatchUp |= gcHelper.compressor.lz4Compress(c, gcHelper, mc, logger);
            }
        }
        return didCatchUp;
    }

    boolean compressSomeChunk(MutatorCatchup mc) {
        return gcHelper.compressor.lz4Compress(selectChunkToCompress(), gcHelper, mc, logger);
    }

    private StableValChunk selectChunkToCompress() {
        double lowestCb = Double.MAX_VALUE;
        StableValChunk mostStableChunk = null;
        for (StableChunk c : chunks.values()) {
            if (!(c instanceof StableValChunk) || c.compressed || c.size() < MIN_SIZE_TO_COMPRESS) {
                continue;
            }
            final StableValChunk valChunk = (StableValChunk) c;
            final double cb = valChunk.cachedCostBenefit();
            if (cb < lowestCb) {
                mostStableChunk = valChunk;
                lowestCb = cb;
            }
        }
        return mostStableChunk;
    }

    private void validateMetadata(KeyHandle kh, Tracker tr) {
        final int chunkGarbageCount = chunkGarbageCount(kh);
        if (tr.garbageCount() != chunkGarbageCount) {
            throw new HotRestartException(String.format(
                    "Global != local garbage count for %s (live in #%03x): %d != %d",
                    kh, tr.chunkSeq(), tr.garbageCount(), chunkGarbageCount));
        }
        if (tr.isTombstone() != chunk(tr.chunkSeq()).records.get(kh).isTombstone()) {
            throw new HotRestartException("Tombstone flags mismatch");
        }
    }

    private int chunkGarbageCount(KeyHandle kh) {
        int garbageCount = 0;
        for (StableChunk c : chunks.values()) {
            garbageCount += garbageCount(c, kh);
        }
        if (destChunkMap != null) {
            for (Chunk c : destChunkMap.values()) {
                garbageCount += garbageCount(c, kh);
            }
        }
        return garbageCount + garbageCount(activeValChunk, kh) + garbageCount(activeTombChunk, kh);
    }

    private static int garbageCount(Chunk c, KeyHandle kh) {
        if (c == null) {
            return 0;
        }
        final Record r = c.records.get(kh);
        return r != null ? r.garbageCount() : 0;
    }
}
