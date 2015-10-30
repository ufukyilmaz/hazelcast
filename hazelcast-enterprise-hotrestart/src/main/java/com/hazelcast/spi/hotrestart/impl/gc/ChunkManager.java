package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStore.TombstoneId;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.ChunkSelection;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.selectChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.Evacuator.copyLiveRecords;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages chunk files and contains top-level code of the GC algorithm.
 */
public final class ChunkManager {
    /** Minimum chunk file size to compress. */
    private static final int MIN_SIZE_TO_COMPRESS = 64 * 1024;
    /** Batch size while releasing tombstones. Between batches we catch up and Thread.yield */
    private static final int TOMBSTONE_RELEASING_BATCH_SIZE = 4 * 1024;
    long occupancy;
    long garbage;
    final Map<Long, StableChunk> chunks = new HashMap<Long, StableChunk>();
    final TrackerMap trackers;
    final GcHelper gcHelper;
    final PrefixTombstoneManager pfixTombstoMgr;
    // temporary storage during GC
    Map<Long, Chunk> destChunkMap;
    // temporary storage during GC
    Map<Long, List<TombstoneId>> tombstonesToRelease;
    WriteThroughChunk activeChunk;
    private final GcLogger logger;

    public ChunkManager(GcHelper gcHelper, PrefixTombstoneManager pfixTombstoMgr) {
        this.gcHelper = gcHelper;
        this.logger = gcHelper.logger;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.trackers = gcHelper.newTrackerMap();
    }

    public long trackedKeyCount() {
        return trackers.size();
    }

    public void close() {
        activeChunk.dispose();
        for (Chunk c : chunks.values()) {
            c.dispose();
        }
        trackers.dispose();
    }

    /** Accounts for the active chunk having been inactivated
     * and replaced with a new one. */
    class ReplaceActiveChunk implements Runnable {
        private final WriteThroughChunk fresh;
        private final WriteThroughChunk closed;

        public ReplaceActiveChunk(WriteThroughChunk fresh, WriteThroughChunk closed) {
            this.fresh = fresh;
            this.closed = closed;
        }

        @Override public void run() {
            activeChunk = fresh;
            if (closed == null) {
                return;
            }
            final StableChunk stable = closed.toStableChunk();
            occupancy += stable.size();
            garbage += stable.garbage;
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
            final Tracker tr = trackers.putIfAbsent(keyHandle, activeChunk.seq, false);
            if (tr != null) {
                if (tr.isAlive()) {
                    final Chunk chunk = chunk(tr.chunkSeq());
                    retire(chunk, keyHandle, chunk.records.get(keyHandle));
                }
                tr.newLiveRecord(activeChunk.seq, isTombstone);
            } else if (isTombstone) {
                throw new HotRestartException("Attempted to add a tombstone for non-existing key");
            }
            activeChunk.addStep2(prefix, keyHandle, seq, size, isTombstone);
        }

        @Override public String toString() {
            return "(" + keyHandle + ',' + seq + ',' + size + ',' + isTombstone + ')';
        }
    }

    long retire(Chunk chunk, KeyHandle kh, Record r) {
        final long keyPrefix = r.keyPrefix(kh);
        adjustGlobalGarbage(chunk, r);
        chunk.retire(kh, r);
        return keyPrefix;
    }

    void retireTombstone(KeyHandle kh, long chunkSeq) {
        final Chunk chunk = chunk(chunkSeq);
        final Record r = chunk.records.get(kh);
        final long seq = r.liveSeq();
        final long keyPrefix = retire(chunk, kh, r);
        trackers.remove(kh);
        submitForRelease(keyPrefix, kh, seq);
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
            } else if (!tr.isAlive()) {
                trackers.remove(kh);
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
            } else if (!tr.isAlive()) {
                trackers.remove(kh);
            }
        }
    }

    void dismissPrefixGarbage(Chunk chunk, KeyHandle kh, Record r) {
        final Tracker tr = trackers.get(kh);
        if (r.isAlive()) {
            adjustGlobalGarbage(chunk, r);
            chunk.retire(kh, r, false);
            tr.retire();
        }
        if (tr != null) {
            tr.decrementGarbageCount(r.garbageCount());
            if (tr.garbageCount() == 0 && !tr.isAlive()) {
                trackers.remove(kh);
            }
        } else {
            assert r.garbageCount() == 0 : "Inconsistent global zero garbage count vs. local " + r.garbageCount();
        }
        r.setGarbageCount(0);
    }

    void submitForRelease(long prefix, KeyHandle kh, long tombstoneSeq) {
        List<TombstoneId> tombIds = tombstonesToRelease.get(prefix);
        if (tombIds == null) {
            tombIds = new ArrayList<TombstoneId>();
            tombstonesToRelease.put(prefix, tombIds);
        }
        tombIds.add(new TombstoneIdImpl(kh, tombstoneSeq));
    }

    private void adjustGlobalGarbage(Chunk chunk, Record r) {
        if (chunk != activeChunk) {
            // Garbage in the active chunk will be accounted for upon deactivation
            garbage += r.size();
        }
    }

    private Chunk chunk(long chunkSeq) {
        if (activeChunk != null && chunkSeq == activeChunk.seq) {
            return activeChunk;
        } else {
            final StableChunk c = chunks.get(chunkSeq);
            return c != null ? c : destChunkMap != null ? destChunkMap.get(chunkSeq) : null;
        }
    }

    private static int garbageCount(Chunk c, KeyHandle kh) {
        if (c == null) {
            return 0;
        }
        final Record r = c.records.get(kh);
        return r != null ? r.garbageCount() : 0;
    }

    GcParams gcParams() {
        return GcParams.gcParams(garbage, occupancy, gcHelper.recordSeq());
    }

    @SuppressWarnings("checkstyle:innerassignment")
    boolean gc(GcParams gcp, GcExecutor.MutatorCatchup mc) {
        final long start = System.nanoTime();
        final ChunkSelection selected;
        if (gcp == GcParams.ZERO || (selected = selectChunksToCollect(
                chunks.values(), gcp, pfixTombstoMgr, mc, logger)).srcChunks.isEmpty()) {
            return false;
        }
        logger.fine("GC: garbage %,d; live %,d; costGoal %,d; reclamationGoal %,d; minCostBenefit %,.2f",
                garbage, occupancy - garbage, gcp.costGoal, gcp.reclamationGoal, gcp.minCostBenefit);
        if (gcp.forceGc) {
            logger.fine("GC: forcing due to ratio %.2f", (float) garbage / (occupancy - garbage));
        }
        this.tombstonesToRelease = new HashMap<Long, List<TombstoneId>>();
        final List<StableChunk> destChunks = copyLiveRecords(selected, this, mc, logger, start);
        releaseTombstones();
        long sizeBefore = 0;
        for (StableChunk src : selected.srcChunks) {
            chunks.remove(src.seq);
            sizeBefore += src.size();
            src.dispose();
        }
        long sizeAfter = 0;
        for (StableChunk dest : destChunks) {
            chunks.put(dest.seq, dest);
            sizeAfter += dest.size();
        }
        this.destChunkMap = null;
        final long reclaimed = sizeBefore - sizeAfter;
        occupancy -= reclaimed;
        garbage -= reclaimed;
        logger.info("GC: reclaimed %,d at cost %,d in %,d ms; garbage %,d; live %,d",
                reclaimed, sizeAfter, NANOSECONDS.toMillis(System.nanoTime() - start), garbage, occupancy - garbage);
        assert garbage >= 0 : String.format("Garbage went below zero: %,d", garbage);
        assert occupancy >= garbage : String.format("Live went below zero: %,d", occupancy - garbage);
        return true;
    }

    void releaseTombstones() {
        for (Entry<Long, List<TombstoneId>> e : tombstonesToRelease.entrySet()) {
            final List<TombstoneId> ids = e.getValue();
            final RamStore ramStore = gcHelper.ramStoreRegistry.ramStoreForPrefix(e.getKey());
            if (ramStore == null) {
                continue;
            }
            for (int i = 0; i < ids.size(); i += TOMBSTONE_RELEASING_BATCH_SIZE) {
                ramStore.releaseTombstones(ids.subList(i, Math.min(ids.size(), i + TOMBSTONE_RELEASING_BATCH_SIZE)));
            }
        }
        tombstonesToRelease = null;
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

    private StableChunk selectChunkToCompress() {
        double lowestCb = Double.MAX_VALUE;
        StableChunk mostStableChunk = null;
        for (StableChunk c : chunks.values()) {
            if (c.compressed || c.size() < MIN_SIZE_TO_COMPRESS) {
                continue;
            }
            final double cb = c.cachedCostBenefit();
            if (cb < lowestCb) {
                mostStableChunk = c;
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
        return garbageCount + garbageCount(activeChunk, kh);
    }

    private static class TombstoneIdImpl implements TombstoneId {
        private final KeyHandle keyHandle;
        private final long tombstoneSeq;

        TombstoneIdImpl(KeyHandle keyHandle, long tombstoneSeq) {
            this.keyHandle = keyHandle;
            this.tombstoneSeq = tombstoneSeq;
        }

        @Override public KeyHandle keyHandle() {
            return keyHandle;
        }

        @Override public long tombstoneSeq() {
            return tombstoneSeq;
        }
    }
}
