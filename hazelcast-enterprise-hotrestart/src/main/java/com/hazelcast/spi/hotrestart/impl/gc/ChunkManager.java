/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BYTES_PER_RECORD_SEQ_INCREMENT;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.decrementGarbageCount;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.incrementGarbageCount;
import static com.hazelcast.spi.hotrestart.impl.gc.ChunkManager.GcParams.gcParams;
import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.selectChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.Compression.lz4Compress;
import static com.hazelcast.spi.hotrestart.impl.gc.Evacuator.copyLiveRecords;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages chunk files and contains top-level code of the GC algorithm.
 */
public class ChunkManager {
    /** Minimum file size to compress. */
    public static final int MIN_SIZE_TO_COMPRESS = 32 * 1024;
    long occupancy;
    long garbage;
    final HashSet<StableChunk> chunks = new HashSet<StableChunk>();
    final HashMap<KeyHandle, Long> garbageKeyCounts = new HashMap<KeyHandle, Long>();
    final HashMap<KeyHandle, Record> liveTombstones = new HashMap<KeyHandle, Record>();
    final GcHelper chunkFactory;
    private WriteThroughChunk activeChunk;

    public ChunkManager(GcHelper chunkFactory) {
        this.chunkFactory = chunkFactory;
    }

    /** Accounts for the active chunk having been inactivated and replaced with
     * a new one. */
    class ReplaceActiveChunk implements GcExecutor.Task {
        private final WriteThroughChunk fresh;
        private final WriteThroughChunk closed;

        public ReplaceActiveChunk(WriteThroughChunk fresh, WriteThroughChunk closed) {
            this.fresh = fresh;
            this.closed = closed;
        }

        @Override public void perform() {
            activeChunk = fresh;
            if (closed == null) {
                return;
            }
            final StableChunk stable = closed.toStableChunk();
            occupancy += stable.size();
            garbage += stable.garbage;
            chunks.add(stable);
        }
    }

    /** Accounts for a record being replaced by a fresh one. */
    class ReplaceRecord implements GcExecutor.Task {
        private final Record stale;
        private final Record fresh;

        public ReplaceRecord(Record stale, Record fresh) {
            this.stale = stale;
            this.fresh = fresh;
        }

        @Override public void perform() {
            if (stale == null) {
                retireTombstone(fresh.keyHandle);
            } else {
                retire(stale);
            }
            add(fresh);
        }
    }

    void retireTombstone(KeyHandle tombKey) {
        final Record tombstone = liveTombstones.remove(tombKey);
        if (tombstone == null) {
            return;
        }
        if (tombstone.chunk != activeChunk) {
            garbage += tombstone.size();
        }
        tombstone.chunk.retire(tombstone);
    }

    void retire(Record stale) {
        if (stale.chunk != activeChunk) {
            garbage += stale.size();
        }
        incrementGarbageCount(garbageKeyCounts, stale);
        stale.chunk.retire(stale);
    }

    void add(Record fresh) {
        fresh.chunk.records.put(fresh.seq, fresh);
        if (fresh.isTombstone()) {
            liveTombstones.put(fresh.keyHandle, fresh);
        }
    }

    GcParams gParams() {
        return gcParams(garbage, occupancy, chunkFactory.recordSeq());
    }

    void dismissGarbage(Chunk c) {
        final Map<KeyHandle, Long> globalCounts = garbageKeyCounts;
        for (Entry<KeyHandle, Long> e : c.garbageKeyCounts.entrySet()) {
            final KeyHandle key = e.getKey();
            final long newVal = globalCounts.get(key) - e.getValue();
            if (newVal > 0) {
                globalCounts.put(key, newVal);
            } else if (newVal == 0) {
                globalCounts.remove(key);
                retireTombstone(key);
            } else {
                throw new HotRestartException("Garbage key count went below zero: " + newVal);
            }
        }
        c.garbageKeyCounts.clear();
        c.garbage = 0;
    }

    void dismissGarbageRecord(Chunk c, Record r) {
        decrementGarbageCount(c.garbageKeyCounts, r);
        if (decrementGarbageCount(garbageKeyCounts, r)) {
            retireTombstone(r.keyHandle);
        }
        c.garbage -= r.size();
    }

    @SuppressWarnings("checkstyle:innerassignment")
    boolean gc(GcParams gcp, GcExecutor.MutatorCatchup mc) {
        final long start = System.nanoTime();
        final List<StableChunk> srcChunks;
        if (gcp == ChunkManager.GcParams.ZERO || (srcChunks = selectChunksToCollect(chunks, gcp, mc)).isEmpty()) {
            return false;
        }
        System.err.printf("GC: garbage %,d; live %,d; costGoal %,d; reclamationGoal %,d; minCostBenefit %,.2f%n",
                garbage, occupancy - garbage, gcp.costGoal, gcp.reclamationGoal, gcp.minCostBenefit);
        final List<StableChunk> destChunks = copyLiveRecords(srcChunks, chunkFactory, mc);
        long sizeBefore = 0;
        for (StableChunk src : srcChunks) {
            chunks.remove(src);
            sizeBefore += src.size();
        }
        long sizeAfter = 0;
        for (StableChunk dest : destChunks) {
            chunks.add(dest);
            sizeAfter += dest.size();
        }
        final long reclaimed = sizeBefore - sizeAfter;
        occupancy -= reclaimed;
        garbage -= reclaimed;
        System.err.printf("GC: reclaimed %,d at cost %,d in %,d ms; garbage %,d; live %,d%n--------------%n",
                reclaimed, sizeAfter, NANOSECONDS.toMillis(System.nanoTime() - start), garbage, occupancy - garbage);
        return true;
    }

    boolean compressAllChunks(MutatorCatchup mc) {
        boolean didCatchUp = false;
        for (StableChunk c : chunks) {
            if (!c.compressed) {
                didCatchUp |= lz4Compress(c, chunkFactory, mc);
            }
        }
        return didCatchUp;
    }

    boolean compressSomeChunk(MutatorCatchup mc) {
        return lz4Compress(selectChunkToCompress(), chunkFactory, mc);
    }

    private StableChunk selectChunkToCompress() {
        double lowestCb = Double.MAX_VALUE;
        StableChunk mostStableChunk = null;
        for (StableChunk c : chunks) {
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


    /**
     * Contains GC ergonomics logic: when to GC and how much to GC.
     */
    static final class GcParams {
        public static final double START_COLLECTING_THRESHOLD = 0.1;
        public static final double FORCE_COLLECTING_THRESHOLD = 0.5;
        public static final double NORMAL_MIN_CB = 20 * (Chunk.SIZE_LIMIT / BYTES_PER_RECORD_SEQ_INCREMENT);
        public static final double FORCED_MIN_CB = 1e-2;
        public static final long MIN_GARBAGE_TO_FORCE_GC = 10 * Chunk.SIZE_LIMIT;
        public static final int COST_GOAL_CHUNKS = 2;
        static final GcParams ZERO = new GcParams(0, 0, 0.0, 0, false);
        static final int SRC_CHUNKS_GOAL = 3;

        final long costGoal;
        final long currChunkSeq;
        final double minCostBenefit;
        final long reclamationGoal;
        final boolean forceGc;
        final boolean limitSrcChunks;

        private GcParams(long garbage, long liveData, double ratio, long currChunkSeq, boolean forceGc) {
            this.currChunkSeq = currChunkSeq;
            this.forceGc = forceGc;
            final long costGoalBytes = COST_GOAL_CHUNKS * Chunk.SIZE_LIMIT;
            this.limitSrcChunks = costGoalBytes < liveData;
            this.costGoal = limitSrcChunks ? costGoalBytes : liveData;
            if (forceGc) {
                this.minCostBenefit = FORCED_MIN_CB;
                this.reclamationGoal = garbageExceedingThreshold(FORCE_COLLECTING_THRESHOLD, garbage, liveData);
            } else {
                final double excessRatio = (ratio - START_COLLECTING_THRESHOLD)
                        / (FORCE_COLLECTING_THRESHOLD - START_COLLECTING_THRESHOLD);
                this.minCostBenefit = Math.max(FORCED_MIN_CB,
                        NORMAL_MIN_CB - (NORMAL_MIN_CB - FORCED_MIN_CB) * excessRatio);
                this.reclamationGoal = garbageExceedingThreshold(START_COLLECTING_THRESHOLD, garbage, liveData);
            }
        }

        static GcParams gcParams(long garbage, long occupancy, long currChunkSeq) {
            final long liveData = occupancy - garbage;
            final double ratio = garbage / (double) liveData;
            final boolean forceGc =
                    ratio >= FORCE_COLLECTING_THRESHOLD && garbage >= MIN_GARBAGE_TO_FORCE_GC;
            if (forceGc) {
                System.err.format("Forcing due to ratio %.2f%n", ratio);
            }
            return ratio < START_COLLECTING_THRESHOLD ? ZERO
                    : new GcParams(garbage, liveData, ratio, currChunkSeq, forceGc);
        }

        @Override public String toString() {
            return "(cost goal " + costGoal
                    + ", min cost-benefit " + minCostBenefit
                    + ", reclamation goal " + reclamationGoal
                    + ", forceGc " + forceGc
                    + ')';
        }

        private static long garbageExceedingThreshold(double thresholdRatio, long garbage, long liveData) {
            return 1 + garbage - (long) (thresholdRatio * liveData);
        }
    }
}
