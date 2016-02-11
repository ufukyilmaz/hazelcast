package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;

import java.util.Map;

/**
 * Rebuilds the runtime metadata of the chunk manager when the system
 * is restarting. Each record read from persistent storage must be fed to this
 * class's <code>accept()</code> method. The order of encountering records
 * doesn't matter for correctness, but for efficiency it is preferred that
 * newer records are encountered first.
 */
public final class Rebuilder {
    private final ChunkManager cm;
    private final GcLogger logger;
    private boolean isLoadingTombstones = true;
    private Counter occupancy;
    private Counter garbage;
    private Map<Long, SetOfKeyHandle> tombKeys;
    private RebuildingChunk chunk;
    private long maxSeq;
    private long maxChunkSeq;

    public Rebuilder(ChunkManager chunkMgr, GcLogger logger) {
        this.cm = chunkMgr;
        this.logger = logger;
        this.occupancy = chunkMgr.tombOccupancy;
        this.garbage = chunkMgr.tombGarbage;
    }

    public long maxChunkSeq() {
        return maxChunkSeq;
    }

    public void startValuePhase(Map<Long, SetOfKeyHandle> tombKeys) {
        this.isLoadingTombstones = false;
        this.tombKeys = tombKeys;
        this.occupancy = cm.valOccupancy;
        this.garbage = cm.valGarbage;
    }

    /**
     * Called when another chunk file starts being read.
     * @param seq the sequence id of the chunk file
     * @param compressed whether the file is compressed
     */
    public void startNewChunk(long seq, boolean compressed) {
        finishCurrentChunk();
        if (seq > maxChunkSeq) {
            this.maxChunkSeq = seq;
        }
        if (isLoadingTombstones) {
            assert !compressed : "Attempt to start a compressed tombstone chunk";
            this.chunk = new RebuildingTombChunk(seq, cm.gcHelper.newRecordMap());
        } else {
            this.chunk = new RebuildingValChunk(seq, cm.gcHelper.newRecordMap(), compressed);
        }
    }

    public void preAccept(long seq, int size) {
        occupancy.inc(size);
        if (seq > maxSeq) {
            this.maxSeq = seq;
        }
    }

    /**
     * Called upon encountering another record in the file.
     * @return {@code false} if the accepted record is known to be garbage; {@code true} otherwise.
     */
    public boolean accept(long prefix, KeyHandle kh, long seq, int size) {
        final Tracker tr = cm.trackers.putIfAbsent(kh, chunk.seq, isLoadingTombstones);
        if (tr == null) {
            // We are accepting a record for a yet-unseen key
            chunk.add(prefix, kh, seq, size);
            return true;
        } else {
            final Chunk chunkWithStale = chunk(tr.chunkSeq());
            final Record stale = chunkWithStale.records.get(kh);
            if (seq >= stale.liveSeq()) {
                // We are accepting a record which replaces an existing, now stale record
                (stale.isTombstone() ? cm.tombGarbage : cm.valGarbage).inc(stale.size());
                chunkWithStale.retire(kh, stale);
                chunk.add(prefix, kh, seq, size);
                tr.newLiveRecord(chunk.seq, isLoadingTombstones, cm.trackers, true);
                removeFromTombKeys(prefix, kh);
                return true;
            } else {
                // We are accepting a stale record
                chunk.size += size;
                chunk.garbage += size;
                garbage.inc(size);
                chunk.acceptStale(tr, prefix, kh, seq, size);
                return false;
            }
        }
    }

    /**
     * Called when encountering a record which is interred by a prefix tombstone.
     * @param size size of the record
     */
    public void acceptCleared(int size) {
        chunk.size += size;
        chunk.garbage += size;
        garbage.inc(size);
    }

    private void removeFromTombKeys(long prefix, KeyHandle kh) {
        if (tombKeys == null) {
            return;
        }
        final SetOfKeyHandle khs = tombKeys.get(prefix);
        if (khs != null) {
            khs.remove(kh);
        }
    }

    private Chunk chunk(long chunkSeq) {
        return chunkSeq == chunk.seq ? chunk : cm.chunks.get(chunkSeq);
    }

    /**
     * Called when done reading. Retires any tombstones which are no longer needed.
     */
    public void done() {
        finishCurrentChunk();
        long tombstoneCount = 0;
        long retiredCount = 0;
        for (StableChunk chunk : cm.chunks.values()) {
            if (!(chunk instanceof StableTombChunk)) {
                continue;
            }
            for (RecordMap.Cursor cursor = chunk.records.cursor(); cursor.advance();) {
                final KeyHandle kh = cursor.toKeyHandle();
                final Tracker tr = cm.trackers.get(kh);
                final Record r = cursor.asRecord();
                if (!r.isAlive()) {
                    continue;
                }
                if (tr.garbageCount() > 0) {
                    tombstoneCount++;
                    continue;
                }
                retiredCount++;
                cm.tombGarbage.inc(r.size());
                chunk.retire(kh, r);
                cm.trackers.removeLiveTombstone(kh);
                cm.submitForDeletionAsNeeded(chunk);
            }
        }
        logger.fine("Retired %,d tombstones. There are %,d left. Record seq is %x",
                retiredCount, tombstoneCount, maxSeq);
        cm.deleteGarbageTombChunks(null);
        cm.gcHelper.initRecordSeq(maxSeq);
    }

    private void finishCurrentChunk() {
        if (chunk != null) {
            final StableChunk stable = chunk.toStableChunk();
            cm.chunks.put(stable.seq, stable);
        }
    }

    private abstract static class RebuildingChunk extends GrowingChunk {
        final boolean compressed;

        RebuildingChunk(long seq, RecordMap records, boolean compressed) {
            super(seq, records);
            this.compressed = compressed;
        }

        abstract void add(long prefix, KeyHandle kh, long seq, int size);

        abstract StableChunk toStableChunk();

        final void add0(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone) {
            addStep1(size);
            addStep2(prefix, kh, seq, size, isTombstone);
        }

        void acceptStale(Tracker tr, long prefix, KeyHandle kh, long seq, int size) { }
    }

    private static final class RebuildingValChunk extends RebuildingChunk {
        RebuildingValChunk(long seq, RecordMap records, boolean compressed) {
            super(seq, records, compressed);
        }

        @Override void add(long prefix, KeyHandle kh, long seq, int size) {
            add0(prefix, kh, seq, size, false);
        }

        @Override StableChunk toStableChunk() {
            return new StableValChunk(seq, records, liveRecordCount, size(), garbage, false, compressed);
        }

        @Override void acceptStale(Tracker tr, long prefix, KeyHandle kh, long seq, int size) {
            final Record sameKeyRecord = records.putIfAbsent(prefix, kh, -seq, size, false, 1);
            if (sameKeyRecord != null) {
                sameKeyRecord.incrementGarbageCount();
            }
            tr.incrementGarbageCount();
        }
    }

    private static final class RebuildingTombChunk extends RebuildingChunk {
        RebuildingTombChunk(long seq, RecordMap records) {
            super(seq, records, false);
        }

        @Override void add(long prefix, KeyHandle kh, long seq, int size) {
            add0(prefix, kh, seq, size, true);
        }

        @Override StableChunk toStableChunk() {
            return new StableTombChunk(seq, records, liveRecordCount, size(), garbage);
        }
    }
}
