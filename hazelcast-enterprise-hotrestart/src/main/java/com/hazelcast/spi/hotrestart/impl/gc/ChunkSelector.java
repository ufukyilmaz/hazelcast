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

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.spi.hotrestart.impl.gc.ChunkManager.GcParams.SRC_CHUNKS_GOAL;
import static java.util.Arrays.asList;

/**
 * Chooses which chunks to evacuate.
 */
class ChunkSelector {
    @SuppressWarnings("MagicNumber")
    private static final int INITIAL_TOP_CHUNKS = 32 * ChunkManager.GcParams.COST_GOAL_CHUNKS;
    private static final Comparator<StableChunk> BY_COST_BENEFIT = new Comparator<StableChunk>() {
        @Override public int compare(StableChunk left, StableChunk right) {
            final double leftCb = left.cachedCostBenefit();
            final double rightCb = right.cachedCostBenefit();
            return leftCb == rightCb ? 0 : leftCb < rightCb ? 1 : -1;
        }
    };
    private final Set<StableChunk> allChunks;
    private final ChunkManager.GcParams gcp;
    private final MutatorCatchup mc;

    ChunkSelector(Set<StableChunk> allChunks, ChunkManager.GcParams gcp, MutatorCatchup mc) {
        this.allChunks = allChunks;
        this.gcp = gcp;
        this.mc = mc;
    }

    static List<StableChunk>
    selectChunksToCollect(Set<StableChunk> allChunks, ChunkManager.GcParams gcp, MutatorCatchup mc) {
        return new ChunkSelector(allChunks, gcp, mc).select();
    }

    @SuppressWarnings({ "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity" })
    List<StableChunk> select() {
        final Set<StableChunk> candidates = candidateChunks();
        final List<StableChunk> srcChunks = new ArrayList<StableChunk>();
        if (candidates.isEmpty()) {
            return srcChunks;
        }
        int cost = 0;
        long garbage = 0;
        final int initialChunksToFind = gcp.limitSrcChunks ? INITIAL_TOP_CHUNKS : candidates.size();
        int chunksToFind = initialChunksToFind;
        done: while (true) {
            for (StableChunk c : topChunks(candidates, chunksToFind)) {
                mc.catchupAsNeeded();
                cost += c.cost();
                garbage += c.garbage;
                srcChunks.add(c);
                candidates.remove(c);
                if (cost >= gcp.costGoal && garbage >= gcp.reclamationGoal && srcChunks.size() >= SRC_CHUNKS_GOAL) {
                    System.err.print("GC: reached all goals");
                    break done;
                }
            }
            if (candidates.isEmpty()) {
                if (cost < gcp.costGoal && !gcp.forceGc) {
                    srcChunks.clear();
                    return srcChunks;
                }
                System.err.print("GC: all candidates chosen, some goals not reached");
                break;
            }
            if (srcChunks.size() == initialChunksToFind) {
                if (cost == 0) {
                    System.err.print("GC: max candidates chosen, zero cost");
                    break;
                }
                if (!gcp.forceGc) {
                    System.err.print("GC: max candidates chosen, some goals not reached");
                    break;
                }
            }
            if (chunksToFind < Integer.MAX_VALUE >> 1) {
                chunksToFind <<= 1;
            }
            System.err.println("Finding " + chunksToFind + " more top chunks");
        }
        System.err.printf("; about to reclaim %,d B at cost %,d B from %d chunks out of %d%n",
                garbage, cost, srcChunks.size(), allChunks.size());
//        diagnoseChunks(allChunks, gcp.currChunkSeq);
        return srcChunks;
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
                topChunks.consider(c);
            }
            final StableChunk[] result = new StableChunk[topChunks.size()];
            for (int i = result.length - 1; i >= 0; i--) {
                mc.catchupAsNeeded();
                result[i] = topChunks.pop();
            }
            return asList(result);
        }
    }

    private static void printSrcChunks(List<StableChunk> srcChunks) {
        System.err.print("From chunks: ");
        for (StableChunk chosen : srcChunks) {
            System.err.print(chosen.seq + " ");
        }
        System.err.println();
    }

    static void diagnoseChunks(Collection<StableChunk> chunks, long currSeq) {
        final StableChunk[] sorted = chunks.toArray(new StableChunk[chunks.size()]);
        for (StableChunk c : chunks) {
            c.updateCostBenefit(currSeq);
        }
        Arrays.sort(sorted, BY_COST_BENEFIT);
        System.err.println("seq   garbage      cost   count  youngestSeq     costBenefit");
        for (StableChunk c : sorted) {
            System.err.printf("%3d %,9d %,9d %,7d %,12d %,15.2f%n",
                    c.seq, c.garbage, c.cost(), c.records.size(), c.youngestRecordSeq, c.cachedCostBenefit());
        }
    }
}
