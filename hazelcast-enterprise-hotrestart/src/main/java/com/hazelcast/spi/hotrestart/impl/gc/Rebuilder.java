package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore.TombstoneId;
import com.hazelcast.util.collection.Long2LongHashMap;

import java.util.HashMap;
import java.util.List;

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
    private Long2LongHashMap prefixTombstones;
    private RebuildingChunk chunk;
    private long maxSeq;

    public Rebuilder(ChunkManager chunkMgr, GcLogger logger, Long2LongHashMap prefixTombstones) {
        this.cm = chunkMgr;
        this.logger = logger;
        this.prefixTombstones = prefixTombstones;
    }

    /**
     * Called when another chunk file starts being read.
     * @param seq the sequence id of the chunk file
     * @param gzipped whether the file is compressed
     */
    public void startNewChunk(long seq, boolean gzipped) {
        finishCurrentChunk();
        chunk = new RebuildingChunk(seq, cm.gcHelper.newRecordMap(), gzipped);
    }

    public long currChunkSeq() {
        return chunk.seq;
    }

    /**
     * Called upon encountering another record in the file.
     * @return {@code false} if the accepted record is known to be garbage; {@code true} otherwise.
     */
    @SuppressWarnings("checkstyle:nestedifdepth")
    public boolean accept(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone) {
        cm.occupancy += size;
        if (seq > maxSeq) {
            maxSeq = seq;
        }
        if (seq <= prefixTombstones.get(prefix)) {
            // We are accepting a cleared record (interred by a prefix tombstone)
            acceptCleared(size);
            return false;
        }
        final Tracker tr = cm.trackers.putIfAbsent(kh, chunk.seq, isTombstone);
        if (tr == null) {
            // We are accepting a record for a yet-unseen key
            chunk.add(prefix, kh, seq, size, isTombstone);
            return true;
        } else {
            final Chunk chunkWithStale = chunk(tr.chunkSeq());
            final Record stale = chunkWithStale.records.get(kh);
            if (seq >= stale.liveSeq()) {
                // We are accepting a record which replaces an existing, now stale record
                cm.garbage += stale.size();
                chunkWithStale.retire(kh, stale);
                chunk.add(prefix, kh, seq, size, isTombstone);
                tr.newLiveRecord(chunk.seq, isTombstone);
                return true;
            } else {
                // We are accepting a stale record
                chunk.size += size;
                chunk.garbage += size;
                cm.garbage += size;
                if (!isTombstone) {
                    final Record sameKeyRecord = chunk.records.putIfAbsent(prefix, kh, 0, 0, false, 1);
                    if (sameKeyRecord != null) {
                        sameKeyRecord.incrementGarbageCount();
                    }
                    tr.incrementGarbageCount();
                }
                return false;
            }
        }
    }

    /**
     * Called when encountering a record with a key prefix whose RAM store
     * no longer exists (i.e., has been destroyed).
     * @param size size of the record
     */
    public void acceptCleared(int size) {
        chunk.size += size;
        chunk.garbage += size;
        cm.garbage += size;
    }

    private Chunk chunk(long chunkSeq) {
        return chunkSeq == chunk.seq ? chunk : cm.chunks.get(chunkSeq);
    }

    /**
     * Called when done reading. Retires any tombstones which
     * are no longer needed.
     */
    public void done() {
        finishCurrentChunk();
        cm.tombstonesToRelease = new HashMap<Long, List<TombstoneId>>();
        long tombstoneCount = 0;
        long retiredCount = 0;
        for (TrackerMap.Cursor cursor = cm.trackers.cursor(); cursor.advance();) {
            final Tracker tr = cursor.asTracker();
            if (!tr.isTombstone()) {
                continue;
            }
            if (tr.garbageCount() > 0) {
                tombstoneCount++;
                continue;
            }
            retiredCount++;
            final KeyHandle kh = cursor.toKeyHandle();
            final Chunk chunk = cm.chunks.get(tr.chunkSeq());
            final Record r = chunk.records.get(kh);
            final long seq = r.liveSeq();
            final long keyPrefix = r.keyPrefix(kh);
            cm.garbage += r.size();
            chunk.retire(kh, r);
            cursor.remove();
            cm.submitForRelease(keyPrefix, kh, seq);
        }
        cm.releaseTombstones();
        logger.fine("Retired " + retiredCount + " tombstones. There are " + tombstoneCount + " left");
        cm.gcHelper.initRecordSeq(maxSeq);
    }

    private void finishCurrentChunk() {
        if (chunk != null) {
            final StableChunk stable = chunk.toStableChunk();
            cm.chunks.put(stable.seq, stable);
        }
    }

    private static class RebuildingChunk extends GrowingChunk {
        private long youngestSeq;
        private boolean compressed;

        RebuildingChunk(long seq, RecordMap records, boolean compressed) {
            super(seq, records);
            this.compressed = compressed;
        }

        final boolean add(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone) {
            final boolean ret = addStep1(size);
            youngestSeq = seq;
            addStep2(prefix, kh, seq, size, isTombstone);
            return ret;
        }

        StableChunk toStableChunk() {
            return new StableChunk(seq, records, liveRecordCount, youngestSeq, size(), garbage, false, compressed);
        }
    }
}
