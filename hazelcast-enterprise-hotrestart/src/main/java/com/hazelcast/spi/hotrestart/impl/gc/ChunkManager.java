package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.GrowingChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.Tracker;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.hotrestart.impl.gc.TombChunkSelector.selectTombChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.TombEvacuator.evacuate;
import static com.hazelcast.spi.hotrestart.impl.gc.ValChunkSelector.selectChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.ValEvacuator.evacuate;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages chunk files and contains top-level code of the GC algorithm.
 */
public final class ChunkManager implements Disposable {
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
    Long2ObjectHashMap<WriteThroughChunk> survivors;
    ActiveValChunk activeValChunk;
    WriteThroughTombChunk activeTombChunk;
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

    @Override public void dispose() {
        activeValChunk.dispose();
        activeTombChunk.dispose();
        for (Chunk c : chunks.values()) {
            c.dispose();
        }
        trackers.dispose();
        gcHelper.dispose();
    }

    /** Accounts for the active value chunk having been inactivated
     * and replaced with a new one. */
    class ReplaceActiveChunk implements Runnable {
        private final ActiveChunk fresh;
        private final ActiveChunk closed;
        private final boolean isTombChunk;

        public ReplaceActiveChunk(ActiveChunk fresh, ActiveChunk closed) {
            this.fresh = fresh;
            this.closed = closed;
            this.isTombChunk = fresh instanceof WriteThroughTombChunk;
        }

        @Override public void run() {
            if (isTombChunk) {
                activeTombChunk = (WriteThroughTombChunk) fresh;
            } else {
                activeValChunk = (ActiveValChunk) fresh;
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
            activeChunk.addStep2(prefix, keyHandle, seq, size);
        }

        @Override public String toString() {
            return "(" + keyHandle + ',' + seq + ',' + size + ',' + isTombstone + ')';
        }
    }

    void retire(Chunk chunk, KeyHandle kh, Record r) {
        adjustGlobalGarbage(chunk, r);
        chunk.retire(kh, r);
    }

    void dismissPrefixGarbage(Chunk chunk, KeyHandle kh, Record r) {
        final Tracker tr = trackers.get(kh);
        if (r.isAlive()) {
            if (tr == null || tr.chunkSeq() != chunk.seq) {
                return;
            }
            adjustGlobalGarbage(chunk, r);
            chunk.retire(kh, r, false);
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

    private void dismissGarbage(Chunk c, MutatorCatchup mc) {
        if (!(c instanceof StableValChunk)) {
            return;
        }
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
            mc.catchupAsNeeded();
        }
        c.garbage = 0;
        mc.catchupNow();
    }

    private void dismissChunkGarbageForKey(KeyHandle kh, Record r, Tracker tr) {
        tr.decrementGarbageCount(r.garbageCount());
        r.setGarbageCount(0);
        final long newCount = tr.garbageCount();
        if (newCount == 0) {
            if (tr.isTombstone()) {
                dismissTombstone(kh, tr.chunkSeq());
            } else {
                trackers.removeIfDead(kh, tr);
            }
        } else {
            assert newCount >= 0
                    : String.format("Garbage count for %s (live in #%03x) went below zero: %d - %d = %d",
                        kh, tr.chunkSeq(), tr.garbageCount(), r.garbageCount(), newCount);
        }
    }

    private void dismissTombstone(KeyHandle kh, long chunkSeq) {
        final Chunk chunk = chunk(chunkSeq);
        final Record r = chunk.records.get(kh);
        retire(chunk, kh, r);
        trackers.removeLiveTombstone(kh);
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
            final Chunk chunk = c != null ? c : survivors != null ? survivors.get(chunkSeq) : null;
            assert chunk != null : String.format("Failed to fetch the chunk #%03x", chunkSeq);
            return chunk;
        }
    }

    GcParams gcParams() {
        return GcParams.gcParams(valGarbage.get(), valOccupancy.get(), gcHelper.chunkSeq());
    }

    boolean valueGc(GcParams gcp, MutatorCatchup mc) {
        if (gcp == GcParams.ZERO) {
            return false;
        }
        final long start = System.nanoTime();
        final Collection<StableValChunk> srcChunks =
                selectChunksToCollect(chunks.values(), gcp, pfixTombstoMgr, mc, logger);
        if (srcChunks.isEmpty()) {
            return false;
        }
        logger.fine("ValChunk selection took %,d us", NANOSECONDS.toMicros(System.nanoTime() - start));
        final long garbage = valGarbage.get();
        final long live = valOccupancy.get() - garbage;
        final double garbagePercent = UNIT_PERCENTAGE * garbage / live;
        logger.fine("Start ValueGC: g/l %2.0f%% (%,d/%,d); costGoal %,d; benefitGoal %,d",
                garbagePercent, garbage, live, gcp.costGoal, gcp.benefitGoal);
        if (gcp.forceGc) {
            logger.info("Forced ValueGC due to g/l %2.0f%%", garbagePercent);
        }
        evacuate(srcChunks, this, mc, logger, start);
        afterEvacuation("Value", srcChunks, valGarbage, valOccupancy, start, mc);
        return true;
    }

    boolean tombGc(MutatorCatchup mc) {
        final long start = System.nanoTime();
        final Collection<StableTombChunk> srcChunks = selectTombChunksToCollect(chunks.values(), mc);
        if (srcChunks.isEmpty()) {
            return false;
        }
        final long garbage = tombGarbage.get();
        final long live = tombOccupancy.get() - garbage;
        logger.fine("Start TombGC: g/l %2.0f%% (%,d/%,d)", UNIT_PERCENTAGE * garbage / live, garbage, live);
        evacuate(srcChunks, this, mc, logger);
        afterEvacuation("Tomb", srcChunks, tombGarbage, tombOccupancy, start, mc);
        return true;
    }

    private void afterEvacuation(
            String gcKind, Collection<? extends StableChunk> evacuatedChunks, Counter garbage, Counter occupancy,
            long start, MutatorCatchup mc
    ) {
        long sizeBefore = 0;
        for (StableChunk evacuated : evacuatedChunks) {
            sizeBefore += evacuated.size();
            gcHelper.deleteChunkFile(evacuated);
            mc.catchupNow();
            dismissGarbage(evacuated, mc);
            evacuated.dispose();
            chunks.remove(evacuated.seq);
        }
        long sizeAfter = 0;
        for (WriteThroughChunk survivor : survivors.values()) {
            sizeAfter += survivor.size();
            // Transfers record ownership from the growing to the stable chunk
            chunks.put(survivor.seq, survivor.toStableChunk());
            mc.catchupNow();
        }
        this.survivors = null;
        final long reclaimed = sizeBefore - sizeAfter;
        final long garbageAfterGc = garbage.inc(-reclaimed);
        final long liveAfterGc = occupancy.inc(-reclaimed) - garbageAfterGc;
        logger.info("%nDone %sGC: took %,3d ms; b/c %3.1f g/l %2.0f%% benefit %,d cost %,d garbage %,d live %,d",
                gcKind, NANOSECONDS.toMillis(System.nanoTime() - start),
                (double) reclaimed / sizeAfter, UNIT_PERCENTAGE * garbageAfterGc / liveAfterGc,
                reclaimed, sizeAfter, garbageAfterGc, liveAfterGc);
        assert garbageAfterGc >= 0 : String.format("%s garbage went below zero: %,d", gcKind, garbageAfterGc);
        assert liveAfterGc >= 0 : String.format("%s live went below zero: %,d", gcKind, liveAfterGc);
    }

}
