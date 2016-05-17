package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.util.collection.HsaHeapMemoryManager;
import com.hazelcast.internal.util.collection.LongSet;
import com.hazelcast.internal.util.collection.LongSetHsa;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.RestartItem;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle.KhCursor;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.GrowingChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.Tracker;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMapBase;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Rebuilds the runtime metadata of the chunk manager when the system
 * is restarting. Each record read from persistent storage must be fed to this
 * class's <code>accept()</code> method. The order of encountering records
 * doesn't matter for correctness, but for efficiency it is preferred that
 * newer records are encountered first.
 */
public final class Rebuilder {
    private final ChunkManager cm;
    private final GcHelper gcHelper;
    private final GcLogger logger;

    private boolean isLoadingTombstones = true;
    private Counter occupancy;
    private Counter garbage;
    private Map<Long, SetOfKeyHandle> tombKeys;
    private Long2ObjectHashMap<RebuildingChunk> rebuildingChunks = new Long2ObjectHashMap<RebuildingChunk>(-1);
    private long maxSeq;
    private long maxChunkSeq;

    @Inject
    private Rebuilder(ChunkManager cm, GcHelper gcHelper, GcLogger logger) {
        this.cm = cm;
        this.gcHelper = gcHelper;
        this.logger = logger;
        this.occupancy = cm.tombOccupancy;
        this.garbage = cm.tombGarbage;
    }

    public long maxChunkSeq() {
        return maxChunkSeq;
    }

    public void startValuePhase(Map<Long, SetOfKeyHandle> tombKeys) {
        closeRebuildingChunks();
        rebuildingChunks.clear();
        this.tombKeys = tombKeys;
        isLoadingTombstones = false;
        occupancy = cm.valOccupancy;
        garbage = cm.valGarbage;
    }

    public void preAccept(long seq, int size) {
        occupancy.inc(size);
        if (seq > maxSeq) {
            this.maxSeq = seq;
        }
    }

    /**
     * Called when encountering a record which is interred by a prefix tombstone.
     */
    public void acceptCleared(RestartItem item) {
        final RebuildingChunk chunk = rebuildingChunk(item.chunkSeq);
        chunk.acceptStale1(item.size);
        chunk.acceptClearedPrefix(item.prefix);
        garbage.inc(item.size);
    }

    /**
     * Called upon encountering another record in the file.
     * @return {@code false} if the accepted record is known to be garbage; {@code true} otherwise.
     */
    public boolean accept(RestartItem item) {
        final long chunkSeq = item.chunkSeq;
        final long prefix = item.prefix;
        final KeyHandle kh = item.keyHandle;
        final long recordSeq = item.recordSeq;
        final int filePos = item.filePos;
        final int size = item.size;
        final RebuildingChunk chunk = rebuildingChunk(chunkSeq);
        final Tracker tr = cm.trackers.putIfAbsent(kh, chunk.seq, isLoadingTombstones);
        if (tr == null) {
            // We are accepting a record for a yet-unseen key
            chunk.add(prefix, kh, recordSeq, filePos, size);
            return true;
        } else {
            final Chunk chunkWithStale = chunk(tr.chunkSeq());
            final Record stale = chunkWithStale.records.get(kh);
            if (recordSeq >= stale.liveSeq()) {
                // We are accepting a record which replaces an existing, now stale record
                (stale.isTombstone() ? cm.tombGarbage : cm.valGarbage).inc(stale.size());
                chunkWithStale.retire(kh, stale);
                chunk.add(prefix, kh, recordSeq, filePos, size);
                tr.newLiveRecord(chunk.seq, isLoadingTombstones, cm.trackers, true);
                if (!isLoadingTombstones) {
                    removeFromTombKeys(prefix, kh);
                }
                return true;
            } else {
                // We are accepting a stale record
                garbage.inc(size);
                chunk.acceptStale1(size);
                chunk.acceptStale2(tr, prefix, kh, recordSeq, size);
                return false;
            }
        }
    }

    /**
     * Called when done reading. Retires any tombstones which are no longer needed.
     */
    public void done() {
        closeRebuildingChunks();
        rebuildingChunks = null;
        final TrackerMapBase trackerMap = (TrackerMapBase) cm.trackers;
        long tombstoneCount = 0;
        long retiredCount = 0;
        for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
            for (KhCursor cursor = e.getValue().cursor(); cursor.advance();) {
                final KeyHandle kh = cursor.asKeyHandle();
                final Tracker tr = trackerMap.get(kh);
                assert tr.isAlive() : "tr is dead";
                assert tr.isTombstone() : "tr is not a tombstone";
                if (tr.garbageCount() > 0) {
                    tombstoneCount++;
                    continue;
                }
                final StableChunk chunk = cm.chunks.get(tr.chunkSeq());
                final Record r = chunk.records.get(kh);
                trackerMap.removeLiveTombstone(kh);
                cm.tombGarbage.inc(r.size());
                chunk.retire(kh, r);
                retiredCount++;
            }
        }
        logger.info("Retired %,d tombstones, left %,d live ones. Record seq is %x",
                retiredCount, tombstoneCount, maxSeq);
        assert tombstoneCount == trackerMap.liveTombstones.get();
        gcHelper.initRecordSeq(maxSeq);
    }

    private Chunk chunk(long chunkSeq) {
        final RebuildingChunk rebuildingChunk = rebuildingChunks.get(chunkSeq);
        return rebuildingChunk != null ? rebuildingChunk : cm.chunks.get(chunkSeq);
    }

    private RebuildingChunk rebuildingChunk(long chunkSeq) {
        final RebuildingChunk chunk = rebuildingChunks.get(chunkSeq);
        return chunk != null ? chunk : startNewChunk(chunkSeq);
    }

    private RebuildingChunk startNewChunk(long seq) {
        if (seq > maxChunkSeq) {
            this.maxChunkSeq = seq;
        }
        final RecordMap records = gcHelper.newRecordMap(false);
        final RebuildingChunk chunk = isLoadingTombstones
                ? new RebuildingTombChunk(seq, records)
                : new RebuildingValChunk(seq, records);
        rebuildingChunks.put(seq, chunk);
        return chunk;
    }

    private void closeRebuildingChunks() {
        for (RebuildingChunk rebuildingChunk : rebuildingChunks.values()) {
            final StableChunk stable = rebuildingChunk.toStableChunk();
            cm.chunks.put(stable.seq, stable);
        }
    }

    private void removeFromTombKeys(long prefix, KeyHandle kh) {
        final SetOfKeyHandle khs = tombKeys.get(prefix);
        if (khs != null) {
            khs.remove(kh);
        }
    }

    private abstract static class RebuildingChunk extends GrowingChunk {

        RebuildingChunk(long seq, RecordMap records) {
            super(seq, records);
        }

        final void add(long prefix, KeyHandle kh, long seq, int filePos, int size) {
            grow(size);
            addStep2(prefix, kh, seq, filePos, size);
        }

        abstract StableChunk toStableChunk();

        final void acceptStale1(int size) {
            grow(size);
            this.garbage += size;
        }

        void acceptStale2(Tracker tr, long prefix, KeyHandle kh, long seq, int size) {
        }

        void acceptClearedPrefix(long prefix) {
        }
    }

    private static final class RebuildingValChunk extends RebuildingChunk {
        private final LongSet clearedPrefixes = new LongSetHsa(0, new HsaHeapMemoryManager());

        RebuildingValChunk(long seq, RecordMap records) {
            super(seq, records);
        }

        @Override
        StableChunk toStableChunk() {
            return new StableValChunk(seq, records, clearedPrefixes, liveRecordCount, size(), garbage, false);
        }

        @Override
        void acceptStale2(Tracker tr, long prefix, KeyHandle kh, long seq, int size) {
            final Record sameKeyRecord = records.putIfAbsent(prefix, kh, -seq, size, false, 1);
            if (sameKeyRecord != null) {
                sameKeyRecord.incrementGarbageCount();
            }
            tr.incrementGarbageCount();
        }

        @Override
        void acceptClearedPrefix(long prefix) {
            clearedPrefixes.add(prefix);
        }

        @Override
        public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int ignored, int size) {
            insertOrUpdateValue(prefix, kh, seq, size);
        }

        @Override
        protected int determineSizeLimit() {
            return valChunkSizeLimit();
        }
    }

    private static final class RebuildingTombChunk extends RebuildingChunk {

        RebuildingTombChunk(long seq, RecordMap records) {
            super(seq, records);
        }

        @Override
        StableChunk toStableChunk() {
            return new StableTombChunk(seq, records, liveRecordCount, size(), garbage);
        }

        @Override
        public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int filePos, int size) {
            insertOrUpdateTombstone(prefix, kh, seq, filePos, size);
        }

        @Override
        protected int determineSizeLimit() {
            return tombChunkSizeLimit();
        }
    }
}
