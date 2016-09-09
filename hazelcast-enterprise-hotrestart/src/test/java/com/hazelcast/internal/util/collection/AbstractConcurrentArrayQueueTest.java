package com.hazelcast.internal.util.collection;

import com.hazelcast.util.function.Consumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractConcurrentArrayQueueTest {

    // must be a power of two to work
    static final int CAPACITY = 1 << 2;

    AbstractConcurrentArrayQueue<Integer> queue;

    @Test
    public void testOffer() {
        boolean result = queue.offer(1);
        assertTrue(result);
    }

    @Test
    public void testOffer_whenQueueIsFull_thenReject() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        boolean result = queue.offer(23);
        assertFalse(result);
    }

    @Test
    public void testOffer_whenArrayQueueWasCompletelyFilled_thenUpdateHeadCache() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }
        queue.poll();

        boolean result = queue.offer(23);
        assertTrue(result);
    }

    @Test(expected = AssertionError.class)
    public void testOffer_whenNull_thenAssert() {
        queue.offer(null);
    }

    @Test
    public void testPoll() {
        queue.offer(23);
        queue.offer(42);

        int result1 = queue.poll();
        int result2 = queue.poll();

        assertEquals(23, result1);
        assertEquals(42, result2);
    }

    @Test
    public void testDrain() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        queue.drain(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                assertNotNull(integer);
            }
        });
    }

    @Test
    public void testDrainTo() {
        testDrainTo(3, 3);
    }

    @Test
    public void testDrainTo_whenLimitIsLargerThanQueue_thenDrainAllElements() {
        testDrainTo(CAPACITY + 1, CAPACITY);
    }

    @Test
    public void testDrainTo_whenLimitIsZero_thenDoNothing() {
        testDrainTo(0, 0);
    }

    @Test
    public void testDrainTo_whenLimitIsNegative_thenDoNothing() {
        testDrainTo(-1, 0);
    }

    private void testDrainTo(int limit, int expected) {
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        queue.drainTo(result, limit);

        assertEquals(expected, result.size());
    }
}
