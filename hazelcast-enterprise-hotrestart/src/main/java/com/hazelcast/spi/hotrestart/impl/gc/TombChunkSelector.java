package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.TOMB_SIZE_LIMIT;
import static com.hazelcast.spi.hotrestart.impl.gc.StableChunk.BY_BENEFIT_COST_DESC;
import static com.hazelcast.spi.hotrestart.impl.gc.StableTombChunk.benefitToCost;

final class TombChunkSelector {
    private final Collection<StableChunk> allChunks;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;
    private final GcLogger logger;

    private TombChunkSelector(
            Collection<StableChunk> allChunks, PrefixTombstoneManager pfixTombstoMgr, MutatorCatchup mc, GcLogger logger
    ) {
        this.allChunks = allChunks;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.mc = mc;
        this.logger = logger;
    }

    static Collection<StableTombChunk> selectTombChunksToCollect(
            Collection<StableChunk> allChunks, PrefixTombstoneManager pfixTombstoMgr, MutatorCatchup mc, GcLogger logger
    ) {
        return new TombChunkSelector(allChunks, pfixTombstoMgr, mc, logger).select();
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
        Collections.sort(candidates, BY_BENEFIT_COST_DESC);
        final List<StableTombChunk> selected = new ArrayList<StableTombChunk>();
        final long minSize = TOMB_SIZE_LIMIT / 2;
        long size = 0;
        long garbage = 0;
        for (StableTombChunk c : candidates) {
            if (benefitToCost(garbage + c.garbage, size + c.size()) < 1) {
                if (size < minSize) {
                    selected.clear();
                }
                break;
            }
            selected.add(c);
            size += c.size();
            garbage += c.garbage;
        }
        return selected;
    }
}
