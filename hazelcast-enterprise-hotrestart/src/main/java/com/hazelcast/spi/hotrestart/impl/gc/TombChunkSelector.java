package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.tombChunkSizeLimit;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk.BY_BENEFIT_COST_DESC;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk.benefitToCost;

final class TombChunkSelector {
    private final Collection<StableChunk> allChunks;
    private final MutatorCatchup mc;

    private TombChunkSelector(Collection<StableChunk> allChunks, MutatorCatchup mc) {
        this.allChunks = allChunks;
        this.mc = mc;
    }

    static Collection<StableTombChunk> selectTombChunksToCollect(
            Collection<StableChunk> allChunks, MutatorCatchup mc, GcLogger logger
    ) {
        return new TombChunkSelector(allChunks, mc).select();
    }

    private Collection<StableTombChunk> select() {
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
        Collections.sort(candidates, BY_BENEFIT_COST_DESC);
        mc.catchupNow();
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
