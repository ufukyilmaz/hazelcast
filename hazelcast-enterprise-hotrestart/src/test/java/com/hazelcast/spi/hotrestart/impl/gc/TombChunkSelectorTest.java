package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TombChunkSelectorTest {

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    public void noStableTombChunksForSelection_noneSelected() {
        // GIVEN
        List<StableChunk> chunks = Arrays.asList(mock(StableChunk.class));
        GcExecutor.MutatorCatchup mc = mock(GcExecutor.MutatorCatchup.class);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.isEmpty());
    }

    @Test
    public void chunkFullOfGarbage_selected() {
        // GIVEN
        StableChunk fullOfGarbage = c(10, 10);
        List<StableChunk> chunks = Arrays.asList(fullOfGarbage);
        GcExecutor.MutatorCatchup mc = mock(GcExecutor.MutatorCatchup.class);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.containsAll(chunks));
    }

    @Test
    public void almostNoGarbage_minimumNotReached() {
        // GIVEN
        StableChunk littleGarbage = c(10, 1);
        List<StableChunk> chunks = Arrays.asList(littleGarbage);
        GcExecutor.MutatorCatchup mc = mock(GcExecutor.MutatorCatchup.class);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.isEmpty());
    }

    @Test
    public void someGarbage_sizeFactorEvaluated() {
        // GIVEN
        StableChunk bigChunkgMoreThanHalfGarbage = c(1 << 20, 1 << 19 + 1);
        StableChunk smallChunkHalfGarbage = c(10, 5);
        List<StableChunk> chunks = Arrays.asList(bigChunkgMoreThanHalfGarbage, smallChunkHalfGarbage);
        GcExecutor.MutatorCatchup mc = mock(GcExecutor.MutatorCatchup.class);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.contains(bigChunkgMoreThanHalfGarbage));
    }

    @Test
    public void aCoupleOfChunks_correctSelected() {
        // GIVEN
        StableChunk fullOfGarbage = c(10, 10);
        StableChunk halfOfGarbage = c(10, 5);
        StableChunk noGarbage = c(10, 0);
        List<StableChunk> chunks = Arrays.asList(noGarbage, halfOfGarbage, fullOfGarbage);
        GcExecutor.MutatorCatchup mc = mock(GcExecutor.MutatorCatchup.class);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.contains(fullOfGarbage));
        assertTrue(result.contains(halfOfGarbage));
        assertEquals(2, result.size());
    }

    StableTombChunk c(long size, long garbage) {
        return new StableTombChunk(counter.incrementAndGet(), null, counter.incrementAndGet(), size, garbage);
    }

}
