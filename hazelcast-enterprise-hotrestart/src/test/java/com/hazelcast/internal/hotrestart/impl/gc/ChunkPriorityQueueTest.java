package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkPriorityQueueTest {

    private ChunkPriorityQueue q;

    @Before
    public void setup() {
        q = new ChunkPriorityQueue(2);
    }

    @Test
    public void whenOfferOneChunk_thenPopOneChunk() {
        q.offer(mockChunk(1, 10, 9));
        assertNotNull(q.pop());
        assertNull(q.pop());
    }

    @Test
    public void whenOfferWorseBetter_thenPopWorseBetter() {
        final StableValChunk worse = mockChunk(2, 10, 9);
        final StableValChunk better = mockChunk(1, 10, 9);
        q.offer(worse);
        q.offer(better);
        assertSame(worse, q.pop());
        assertSame(better, q.pop());
        assertNull(q.pop());
    }

    @Test
    public void whenOfferBetterWorse_thenPopWorseBetter() {
        final StableValChunk worse = mockChunk(2, 10, 9);
        final StableValChunk better = mockChunk(1, 10, 9);
        q.offer(better);
        q.offer(worse);
        assertSame(worse, q.pop());
        assertSame(better, q.pop());
        assertNull(q.pop());
    }

    @Test
    public void whenOfferWorstMiddleBest_thenPopMiddleBest() {
        final StableValChunk worst = mockChunk(3, 10, 9);
        final StableValChunk middle = mockChunk(2, 10, 9);
        final StableValChunk best = mockChunk(1, 10, 9);
        q.offer(worst);
        q.offer(middle);
        q.offer(best);
        assertSame(middle, q.pop());
        assertSame(best, q.pop());
        assertNull(q.pop());
    }

    @Test
    public void whenOfferBestMiddleWorst_thenPopMiddleBest() {
        final StableValChunk worst = mockChunk(3, 10, 9);
        final StableValChunk middle = mockChunk(2, 10, 9);
        final StableValChunk best = mockChunk(1, 10, 9);
        q.offer(best);
        q.offer(middle);
        q.offer(worst);
        assertSame(middle, q.pop());
        assertSame(best, q.pop());
        assertNull(q.pop());
    }

    private static StableValChunk mockChunk(long seq, long size, long garbage) {
        final StableValChunk c = new StableValChunk(seq, null, 1, size, garbage, false);
        c.updateBenefitToCost(3);
        return c;
    }
}
