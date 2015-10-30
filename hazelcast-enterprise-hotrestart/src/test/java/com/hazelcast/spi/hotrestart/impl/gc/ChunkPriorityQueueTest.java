package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkPriorityQueueTest {
    private ChunkPriorityQueue q;

    @Before public void setup() {
        q = new ChunkPriorityQueue(2);
    }

    @Test public void whenOfferOneChunk_thenPopOneChunk() {
        q.offer(mockChunk(1, 10, 10, 9));
        Assert.assertNotNull(q.pop());
        assertNull(q.pop());
    }

    @Test public void whenOfferWorseBetter_thenPopWorseBetter() {
        final StableChunk worse = mockChunk(1, 1000, 10, 9);
        final StableChunk better = mockChunk(2, 10, 10, 9);
        q.offer(worse);
        q.offer(better);
        assertSame(worse, q.pop());
        assertSame(better, q.pop());
        assertNull(q.pop());
    }

    @Test public void whenOfferBetterWorse_thenPopWorseBetter() {
        final StableChunk worse = mockChunk(1, 1000, 10, 9);
        final StableChunk better = mockChunk(2, 10, 10, 9);
        q.offer(better);
        q.offer(worse);
        assertSame(worse, q.pop());
        assertSame(better, q.pop());
        assertNull(q.pop());
    }

    @Test public void whenOfferWorstMiddleBest_thenPopMiddleBest() {
        final StableChunk worst = mockChunk(1, 1000, 10, 9);
        final StableChunk middle = mockChunk(1, 100, 10, 9);
        final StableChunk best = mockChunk(2, 10, 10, 9);
        q.offer(worst);
        q.offer(middle);
        q.offer(best);
        assertSame(middle, q.pop());
        assertSame(best, q.pop());
        assertNull(q.pop());
    }

    @Test public void whenOfferBestMiddleWorst_thenPopMiddleBest() {
        final StableChunk worst = mockChunk(1, 1000, 10, 9);
        final StableChunk middle = mockChunk(1, 100, 10, 9);
        final StableChunk best = mockChunk(2, 10, 10, 9);
        q.offer(best);
        q.offer(middle);
        q.offer(worst);
        assertSame(middle, q.pop());
        assertSame(best, q.pop());
        assertNull(q.pop());
    }

    private static StableChunk mockChunk(long seq, long youngestRecordSeq, long size, long garbage) {
        final StableChunk c = new StableChunk(seq, null, 1, youngestRecordSeq, size, garbage, false, false);
        c.updateCostBenefit(1000);
        return c;
    }
}
