package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableTombChunk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.tombChunkSizeLimit;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk.BY_BENEFIT_COST_DESC;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.StableTombChunk.benefitToCost;

final class TombChunkSelector {
    private final MutatorCatchup mc;

    private TombChunkSelector(MutatorCatchup mc) {
        this.mc = mc;
    }

    /**
     * Selects tombstone chunks for a TombGC run. All chunks with benefit-cost factor at least 2
     * are selected and further chunks are selected as needed reach at least half a chunk's worth of
     * live data, but no chunk with benefit-cost less than 1 is selected. If the live data size goal
     * cannot be reached, no chunks are selected (the GC cycle will not proceed).
     */
    static Collection<StableTombChunk> selectTombChunksToCollect(
            Collection<? extends StableChunk> allChunks, MutatorCatchup mc) {
        return new TombChunkSelector(mc).select(allChunks);
    }

    private Collection<StableTombChunk> select(Collection<? extends StableChunk> allChunks) {
        final List<StableTombChunk> candidates = candidateChunks(allChunks);
        sortCandidates(candidates);
        return selectChunksForCollection(candidates);
    }

    private List<StableTombChunk> candidateChunks(Collection<? extends StableChunk> allChunks) {
        final List<StableTombChunk> candidates = new ArrayList<StableTombChunk>();
        for (StableChunk chunk : allChunks) {
            if (!(chunk instanceof StableTombChunk)) {
                continue;
            }
            final StableTombChunk c = (StableTombChunk) chunk;
            if (c.size() == 0 || c.garbage > 0) {
                c.updateBenefitToCost();
                candidates.add(c);
            }
        }
        mc.catchupNow();
        return candidates;
    }

    private void sortCandidates(List<StableTombChunk> candidates) {
        Collections.sort(candidates, BY_BENEFIT_COST_DESC);
        mc.catchupNow();
    }

    private Collection<StableTombChunk> selectChunksForCollection(List<StableTombChunk> candidates) {
        final List<StableTombChunk> selected = new ArrayList<StableTombChunk>();
        final long minSize = tombChunkSizeLimit() / 2;
        long size = 0;
        long garbage = 0;
        for (StableTombChunk c : candidates) {
            final double b2c = benefitToCost(garbage + c.garbage, size + c.size());
            if (b2c < 2 && size > minSize) {
                break;
            }
            if (b2c < 1) {
                // If benefit/cost is too low and we still don't have enough data to copy, abort GC cycle
                selected.clear();
                break;
            }
            selected.add(c);
            size += c.size();
            garbage += c.garbage;
        }
        return selected;
    }
}
