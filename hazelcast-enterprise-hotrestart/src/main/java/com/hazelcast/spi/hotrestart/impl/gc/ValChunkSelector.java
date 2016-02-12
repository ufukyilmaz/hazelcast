package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.util.collection.LongHashSet;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.spi.hotrestart.impl.gc.GcParams.MAX_RECORD_COUNT;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk.BY_BENEFIT_COST_DESC;
import static com.hazelcast.util.QuickMath.log2;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Chooses which chunks to evacuate.
 */
final class ValChunkSelector {
    @SuppressWarnings("MagicNumber")
    static final int INITIAL_TOP_CHUNKS = 16 * GcParams.MAX_COST_CHUNKS;
    private static final Comparator<StableValChunk> BY_SEQ_DESC = new Comparator<StableValChunk>() {
        @Override public int compare(StableValChunk left, StableValChunk right) {
            final long leftSeq = left.seq;
            final long rightSeq = right.seq;
            return leftSeq > rightSeq ? -1 : leftSeq < rightSeq ? 1 : 0;
        }
    };
    private final Collection<StableChunk> allChunks;
    private final GcParams gcp;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final GcLogger logger;

    private ValChunkSelector(Collection<StableChunk> allChunks, GcParams gcp, PrefixTombstoneManager pfixTombstoMgr,
                             MutatorCatchup mc, GcLogger logger) {
        this.allChunks = allChunks;
        this.gcp = gcp;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.mc = mc;
        this.logger = logger;
    }

    final List<StableValChunk> srcChunks = new ArrayList<StableValChunk>();

    static Collection<StableValChunk>
    selectChunksToCollect(Collection<StableChunk> allChunks, GcParams gcp,
                          PrefixTombstoneManager pfixTombstoMgr, MutatorCatchup mc, GcLogger logger) {
        return new ValChunkSelector(allChunks, gcp, pfixTombstoMgr, mc, logger).select();
    }

    @SuppressWarnings({ "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity" })
    private Collection<StableValChunk> select() {
        final Set<StableValChunk> candidates = candidateChunks();
        if (candidates.isEmpty()) {
            return candidates;
        }
        long benefit = 0;
        long cost = 0;
        final int initialChunksToFind = gcp.limitSrcChunks ? INITIAL_TOP_CHUNKS : candidates.size();
        int chunksToFind = initialChunksToFind;
        final String status;
        int liveRecordCount = 0;
        done: while (true) {
            for (StableValChunk c : topChunks(candidates, chunksToFind)) {
                mc.catchupAsNeeded();
                pfixTombstoMgr.dismissGarbage(c);
                cost += c.cost();
                liveRecordCount += c.liveRecordCount;
                benefit += c.garbage;
                srcChunks.add(c);
                candidates.remove(c);
                final String statusIfAny = status(benefit, cost, liveRecordCount);
                if (statusIfAny != null) {
                    status = statusIfAny;
                    break done;
                }
            }
            if (candidates.isEmpty()) {
                if (cost > 0 && cost < gcp.minCost) {
                    srcChunks.clear();
                    return srcChunks;
                }
                status = "all candidates chosen, " + (cost == 0 ? "zero cost" : "some goals not reached");
                break;
            }
            if (srcChunks.size() == initialChunksToFind) {
                if (cost == 0) {
                    status = "max candidates chosen, zero cost";
                    break;
                }
                if (!gcp.forceGc) {
                    status = "max candidates chosen, some goals not reached";
                    break;
                }
            }
            if (chunksToFind < Integer.MAX_VALUE >> 1) {
                chunksToFind <<= 1;
            }
            logger.finest("Finding " + chunksToFind + " more top chunks");
        }
        if ((double) benefit / cost < gcp.minBenefitToCost) {
            srcChunks.clear();
            return srcChunks;
        }
        diagnoseChunks(allChunks, srcChunks, gcp, logger);
        logger.fine("GC: %s; about to reclaim %,d B at cost %,d B from %,d chunks out of %,d",
                status, benefit, cost, srcChunks.size(), allChunks.size());
        return srcChunks;
    }

    private Set<StableValChunk> candidateChunks() {
        final Set<StableValChunk> candidates = new HashSet<StableValChunk>();
        for (StableChunk chunk : allChunks) {
            if (!(chunk instanceof StableValChunk)) {
                continue;
            }
            final StableValChunk c = (StableValChunk) chunk;
            if (c.size() == 0 || c.garbage > 0) {
                c.updateBenefitToCost(gcp.currChunkSeq);
                candidates.add(c);
            }
        }
        return candidates;
    }

    private List<StableValChunk> topChunks(Set<StableValChunk> candidates, int limit) {
        if (candidates.size() <= limit) {
            final List<StableValChunk> sortedChunks = new ArrayList<StableValChunk>(candidates);
            Collections.sort(sortedChunks, BY_BENEFIT_COST_DESC);
            mc.catchupNow();
            return sortedChunks;
        } else {
            final ChunkPriorityQueue topChunks = new ChunkPriorityQueue(limit);
            for (StableValChunk c : candidates) {
                mc.catchupAsNeeded();
                topChunks.offer(c);
            }
            final StableValChunk[] result = new StableValChunk[topChunks.size()];
            for (int i = result.length - 1; i >= 0; i--) {
                mc.catchupAsNeeded();
                result[i] = topChunks.pop();
            }
            return asList(result);
        }
    }

    private String status(long garbage, long cost, int liveRecordCount) {
        return cost > gcp.maxCost
                ? format("max cost exceeded: will output %,d bytes", cost)
                : liveRecordCount > MAX_RECORD_COUNT
                ? format("max record count exceeded: will copy %,d records", liveRecordCount)
                : cost >= gcp.costGoal && garbage >= gcp.benefitGoal
                ? "reached all goals"
                : null;
    }

    static void diagnoseChunks(Collection<StableChunk> allChunks, Collection<? extends StableChunk> selectedChunks,
                               GcParams gcp, GcLogger logger
    ) {
        if (!logger.isFinestEnabled()) {
            return;
        }
        final List<StableValChunk> valChunks = new ArrayList<StableValChunk>(allChunks.size());
        int tombChunkCount = 0;
        for (StableChunk chunk : allChunks) {
            if (!(chunk instanceof StableValChunk)) {
                tombChunkCount++;
                continue;
            }
            final StableValChunk c = (StableValChunk) chunk;
            c.updateBenefitToCost(gcp.currChunkSeq);
            valChunks.add(c);
        }
        Collections.sort(valChunks, BY_SEQ_DESC);
        final LongHashSet selectedSeqs = new LongHashSet(selectedChunks.size(), -1);
        for (StableChunk c : selectedChunks) {
            selectedSeqs.add(c.seq);
        }
        final StringWriter sw = new StringWriter(512);
        final PrintWriter o = new PrintWriter(sw);
        o.format("%nValue chunks %,d Tombstone chunks: %,d", valChunks.size(), tombChunkCount);
        o.format("%n seq age        CB factor  recCount%n");
        for (StableValChunk c : valChunks) {
            o.format("%4x %3d %,15.2f    %,7d %s %s%n",
                    c.seq,
                    log2(gcp.currChunkSeq - c.seq),
                    c.cachedBenefitToCost(),
                    c.liveRecordCount,
                    selectedSeqs.contains(c.seq) ? "X" : " ",
                    visualizedChunk(c.garbage, c.size(), valChunkSizeLimit()));
        }
        logger.finest(sw.toString());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static String visualizedChunk(long garbage, long size, int chunkSize) {
        final int chunkLimitChars = 16;
        final int bytesPerChar = chunkSize / chunkLimitChars;
        final StringBuilder b = new StringBuilder(chunkLimitChars * 3 / 2).append('|');
        final long garbageChars = garbage / bytesPerChar;
        final long sizeChars = size / bytesPerChar;
        for (int i = 0; i < sizeChars; i++) {
            b.append(i <= garbageChars ? '-' : '#');
        }
        return b.toString();
    }

}
