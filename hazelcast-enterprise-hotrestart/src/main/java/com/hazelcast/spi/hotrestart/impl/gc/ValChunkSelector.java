package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.internal.util.collection.LongHashSet;

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
import static com.hazelcast.internal.util.QuickMath.log2;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Selects value chunks for a ValueGC run. Adds them one by one in descending order of cost/benefit (CB) score,
 * each time evaluating these constraints:
 * <ol>
 *     <li>
 *         Cost goal. Keep adding chunks until this much live data is selected or a limit is exceeded.
 *         The goal is half the maximum cost (see below).
 *     </li><li>
 *         Benefit goal. Keep adding chunks until this much garbage is selected or a limit is exceeded.
 *         The goal is to collect enough garbage to fall below the "start GC" threshold of 5%. During a Forced GC
 *         the goal is to fall below the Forced GC ratio of 30%.
 *     </li><li>
 *         Minimum cost. If this value wasn't reached before hitting a limit, the GC cycle is aborted.
 *         It is equal to half a chunk, unless the garbage/live ratio has exceeded 20%, then it is zero.
 *     </li><li>
 *         Maximum cost. Stop selecting more chunks when this limit is reached. The limit is 8 chunks.
 *         It does not apply to Forced GC.
 *     </li><li>
 *         Minimum benefit/cost. After chunk selection is done, the expected ratio of benefit to cost for the GC cycle
 *         is evaluated (garbage reclaimed / live data copied). If it is below this threshold, the GC cycle is aborted.
 *         The threshold is a function of the overall garbage/live ratio: it is 5.0 at 5%, falling in a straight line
 *         to 0.4 at 20%, and continuing to fall beyond 20% with the same slope. During Forced GC this threshold is 0.01.
 *     </li>
 * </ol>
 * The values used to evaluate constraints are calculated by {@link GcParams}.
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

    @Inject private PrefixTombstoneManager pfixTombstoMgr;
    @Inject private MutatorCatchup mc;
    @Inject private GcLogger logger;

    ValChunkSelector(Collection<StableChunk> allChunks, GcParams gcp) {
        this.allChunks = allChunks;
        this.gcp = gcp;
    }

    /**
     * Entry point to the selection procedure.
     * @return the chunks selected for a ValueGC run
     */
    @SuppressWarnings({ "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity" })
    Collection<StableValChunk> select() {
        final Set<StableValChunk> candidates = candidateChunks();
        if (candidates.isEmpty()) {
            return candidates;
        }
        final List<StableValChunk> srcChunks = new ArrayList<StableValChunk>();
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
            logger.finestVerbose("Finding " + chunksToFind + " more top chunks");
        }
        if ((double) benefit / cost < gcp.minBenefitToCost) {
            srcChunks.clear();
            return srcChunks;
        }
        diagnoseChunks(allChunks, srcChunks, gcp, logger);
        logger.finest("GC: %s; about to reclaim %,d B at cost %,d B from %,d chunks out of %,d",
                status, benefit, cost, srcChunks.size(), allChunks.size());
        return srcChunks;
    }

    /**
     * Finds all candidate value chunks. Chunks with zero garbage but non-zero size do not qualify as candidates.
     */
    private Set<StableValChunk> candidateChunks() {
        final Set<StableValChunk> candidates = new HashSet<StableValChunk>(allChunks.size());
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

    /**
     * Finds at most {@code limit} best GC candidates.
     * @param candidates all candidates
     * @param limit max number of candidates to return
     */
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

    /**
     * Determines the current status of chunk selection.
     * @param garbage amount of garbage in selected chunks
     * @param cost amount of live data in selected chunks (= GC cost, the amount of data to copy)
     * @param liveRecordCount number of live records in selected chunks
     * @return {@code null} if more chunks should be selected or a string message describing the reason
     *         why enough chunks were selected
     */
    private String status(long garbage, long cost, int liveRecordCount) {
        return cost > gcp.maxCost
                ? format("max cost exceeded: will output %,d bytes", cost)
                : liveRecordCount > MAX_RECORD_COUNT
                ? format("max record count exceeded: will copy %,d records", liveRecordCount)
                : cost >= gcp.costGoal && garbage >= gcp.benefitGoal
                ? "reached all goals"
                : null;
    }

    /**
     * Produces a large, detailed log message that reports some diagnostic values and visually presents all candidate
     * chunks (displays a bar proportional to chunk size, split into garbage and live parts).
     */
    static void diagnoseChunks(Collection<StableChunk> allChunks, Collection<? extends StableChunk> selectedChunks,
                               GcParams gcp, GcLogger logger
    ) {
        if (!logger.isFinestVerboseEnabled()) {
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
        logger.finestVerbose(sw.toString());
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
