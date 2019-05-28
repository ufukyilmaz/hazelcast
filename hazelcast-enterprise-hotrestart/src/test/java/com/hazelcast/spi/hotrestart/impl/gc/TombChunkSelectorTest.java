package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createMutatorCatchup;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TombChunkSelectorTest {

    private AtomicInteger counter = new AtomicInteger(1);

    private MutatorCatchup mc;

    @Before
    public void before() {
        mc = createMutatorCatchup();
    }

    @Test
    public void noStableTombChunksForSelection_noneSelected() {
        // GIVEN
        List<StableChunk> chunks = singletonList(mock(StableChunk.class));

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.isEmpty());
    }

    @Test
    public void chunkFullOfGarbage_selected() {
        // GIVEN
        StableChunk fullOfGarbage = c(10, 10);
        List<StableChunk> chunks = singletonList(fullOfGarbage);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.containsAll(chunks));
    }

    @Test
    public void almostNoGarbage_minimumNotReached() {
        // GIVEN
        StableChunk littleGarbage = c(10, 1);
        List<StableChunk> chunks = singletonList(littleGarbage);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.isEmpty());
    }

    @Test
    public void someGarbage_sizeFactorEvaluated() {
        // GIVEN
        StableTombChunk bigChunkgMoreThanHalfGarbage = c(1 << 20, 1 << 19 + 1);
        StableTombChunk smallChunkHalfGarbage = c(10, 5);
        List<StableTombChunk> chunks = Arrays.asList(bigChunkgMoreThanHalfGarbage, smallChunkHalfGarbage);

        // WHEN
        Collection<StableTombChunk> result = TombChunkSelector.selectTombChunksToCollect(chunks, mc);

        // THEN
        assertTrue(result.contains(bigChunkgMoreThanHalfGarbage));
    }

    @Test
    public void aCoupleOfChunks_correctSelected() {
        // GIVEN
        StableTombChunk fullOfGarbage = c(10, 10);
        StableTombChunk halfOfGarbage = c(10, 5);
        StableTombChunk noGarbage = c(10, 0);
        List<StableTombChunk> chunks = Arrays.asList(noGarbage, halfOfGarbage, fullOfGarbage);

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
