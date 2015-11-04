package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.spi.hotrestart.impl.gc.GcParams.RECORD_COUNT_LIMIT;
import static com.hazelcast.spi.hotrestart.impl.gc.GcParams.SRC_CHUNKS_GOAL;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Chooses which chunks to evacuate.
 */
final class ChunkSelector {
    @SuppressWarnings("MagicNumber")
    private static final int INITIAL_TOP_CHUNKS = 32 * GcParams.COST_GOAL_CHUNKS;
    private static final Comparator<StableChunk> BY_COST_BENEFIT = new Comparator<StableChunk>() {
        @Override public int compare(StableChunk left, StableChunk right) {
            final double leftCb = left.cachedCostBenefit();
            final double rightCb = right.cachedCostBenefit();
            return leftCb == rightCb ? 0 : leftCb < rightCb ? 1 : -1;
        }
    };
    private final Collection<StableChunk> allChunks;
    private final GcParams gcp;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final GcLogger logger;

    private ChunkSelector(Collection<StableChunk> allChunks, GcParams gcp, PrefixTombstoneManager pfixTombstoMgr,
                          MutatorCatchup mc, GcLogger logger) {
        this.allChunks = allChunks;
        this.gcp = gcp;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.mc = mc;
        this.logger = logger;
    }

    /** Aggregates data returned to the caller of selectChunksToCollect() */
    static class ChunkSelection {
        final List<StableChunk> srcChunks = new ArrayList<StableChunk>();
        int liveRecordCount;
    }

    static ChunkSelection
    selectChunksToCollect(Collection<StableChunk> allChunks, GcParams gcp,
                          PrefixTombstoneManager pfixTombstoMgr, MutatorCatchup mc, GcLogger logger) {
        return new ChunkSelector(allChunks, gcp, pfixTombstoMgr, mc, logger).select();
    }

    @SuppressWarnings({ "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity" })
    private ChunkSelection select() {
        final ChunkSelection cs = new ChunkSelection();
        final Set<StableChunk> candidates = candidateChunks();
        if (candidates.isEmpty()) {
            return cs;
        }
        long cost = 0;
        long garbage = 0;
        final int initialChunksToFind = gcp.limitSrcChunks ? INITIAL_TOP_CHUNKS : candidates.size();
        int chunksToFind = initialChunksToFind;
        final String status;
        done: while (true) {
            for (StableChunk c : topChunks(candidates, chunksToFind)) {
                mc.catchupAsNeeded();
                pfixTombstoMgr.dismissGarbage(c);
                cost += c.cost();
                cs.liveRecordCount += c.liveRecordCount;
                garbage += c.garbage;
                cs.srcChunks.add(c);
                candidates.remove(c);
                if (cs.liveRecordCount > RECORD_COUNT_LIMIT) {
                    status = format("record count limit exceeded: will copy %,d records", cs.liveRecordCount);
                    break done;
                }
                if (cost >= gcp.costGoal && garbage >= gcp.reclamationGoal && cs.srcChunks.size() >= SRC_CHUNKS_GOAL) {
                    status = "reached all goals";
                    break done;
                }
            }
            if (candidates.isEmpty()) {
                if (cost > 0 && cost < gcp.costGoal && !gcp.forceGc) {
                    cs.srcChunks.clear();
                    return cs;
                }
                status = "all candidates chosen, " + (cost == 0 ? "zero cost" : "some goals not reached");
                break;
            }
            if (cs.srcChunks.size() == initialChunksToFind) {
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
        diagnoseChunks(allChunks, gcp.currChunkSeq);
        logger.fine("GC: %s; about to reclaim %,d B at cost %,d B from %,d chunks out of %,d",
                status, garbage, cost, cs.srcChunks.size(), allChunks.size());
        return cs;
    }

    private Set<StableChunk> candidateChunks() {
        final Set<StableChunk> candidates = new HashSet<StableChunk>();
        for (StableChunk c : allChunks) {
            if (c.size() > 0 && c.garbage == 0 || c.updateCostBenefit(gcp.currChunkSeq) < gcp.minCostBenefit) {
                continue;
            }
            candidates.add(c);
        }
        return candidates;
    }

    private List<StableChunk> topChunks(Set<StableChunk> candidates, int limit) {
        if (candidates.size() <= limit) {
            final List<StableChunk> sortedChunks = new ArrayList<StableChunk>(candidates);
            Collections.sort(sortedChunks, BY_COST_BENEFIT);
            mc.catchupNow();
            return sortedChunks;
        } else {
            final ChunkPriorityQueue topChunks = new ChunkPriorityQueue(limit);
            for (StableChunk c : candidates) {
                mc.catchupAsNeeded();
                topChunks.offer(c);
            }
            final StableChunk[] result = new StableChunk[topChunks.size()];
            for (int i = result.length - 1; i >= 0; i--) {
                mc.catchupAsNeeded();
                result[i] = topChunks.pop();
            }
            return asList(result);
        }
    }

    private void diagnoseChunks(Collection<StableChunk> chunks, long currSeq) {
        if (!logger.isFinestEnabled()) {
            return;
        }
        final StableChunk[] sorted = chunks.toArray(new StableChunk[chunks.size()]);
        for (StableChunk c : chunks) {
            c.updateCostBenefit(currSeq);
        }
        Arrays.sort(sorted, BY_COST_BENEFIT);
        final PrintWriter o = new PrintWriter(new StringWriter(512));
        o.println("seq    garbage       cost   count  youngestSeq     costBenefit");
        for (StableChunk c : sorted) {
            o.format("%3x %,10d %,10d %,7d %,12d %,15.2f",
                    c.seq, c.garbage, c.cost(), c.records.size(), c.youngestRecordSeq, c.cachedCostBenefit());
        }
        logger.finest(o.toString());
    }
}
