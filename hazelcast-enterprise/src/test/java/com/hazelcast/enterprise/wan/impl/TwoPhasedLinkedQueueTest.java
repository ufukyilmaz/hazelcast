package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.enterprise.wan.impl.FinalizableInteger.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Single-threaded tests of the queue for verifying that the behavior
 * is the expected if all calls to the queue methods were serialized.
 *
 * For the concurrent tests see {@link TwoPhasedLinkedQueueConcurrencyTest}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TwoPhasedLinkedQueueTest {

    private static final int QUEUE_SIZE = 100;
    private static final int HALF_QUEUE_SIZE = 50;

    @Test
    public void testEnqueueOnly() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sumOfEntriesAdded = fillQueue(queue);

        assertSumOfEntries(queue, sumOfEntriesAdded);
    }

    @Test
    public void testEnqueueThrowsOnNull() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        assertThrows(NullPointerException.class, () -> queue.offer(null));
        assertEquals(0, queue.size());
    }

    @Test
    public void testPollFinalizes() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));

        while (queue.poll() != null) {
        }

        assertQueueEmpty(queue);
    }

    @Test
    public void testDequeue() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));
        FinalizableInteger dequeued = queue.dequeue();

        assertNull(queue.dequeue());
        assertEquals(42, dequeued.intValue());

        // test that the entry is waiting for finalization
        assertSumOfEntries(queue, 42);

        // test that finalizing the dequeued entry is successful
        dequeued.doFinalize();
        assertQueueEmpty(queue);
    }

    @Test
    public void testFinalizeOnDequeue() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sumOfEntriesAdded = fillQueue(queue);

        long actual = 0;
        FinalizableInteger dequeued;
        while ((dequeued = queue.dequeue()) != null) {
            actual += dequeued.intValue();
            dequeued.doFinalize();
        }

        assertEquals(sumOfEntriesAdded, actual);
    }

    @Test
    public void testDequeueHalfWithoutFinalize() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sumOfEntriesAdded = fillQueue(queue);

        for (int i = 0; i < HALF_QUEUE_SIZE; i++) {
            queue.dequeue();
        }

        assertSumOfEntries(queue, sumOfEntriesAdded);
    }

    @Test
    public void testDequeueHalfWithDeferredFinalize() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sumOfEntriesAdded = fillQueue(queue);

        AtomicLong actual = new AtomicLong();
        ArrayList<FinalizableInteger> entries = new ArrayList<>(HALF_QUEUE_SIZE);
        for (int i = 0; i < HALF_QUEUE_SIZE; i++) {
            FinalizableInteger dequeued = queue.dequeue();
            entries.add(dequeued);
            actual.getAndAdd(dequeued.intValue());
        }

        entries.forEach(FinalizableInteger::doFinalize);

        queue.consumeAll(e -> actual.addAndGet(e.intValue()));

        assertEquals(sumOfEntriesAdded, actual.get());
    }

    @Test
    public void testOutOfOrderFinalization() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));
        queue.offer(of(24));

        FinalizableInteger dequeuedFirst = queue.dequeue();
        FinalizableInteger dequeuedSecond = queue.dequeue();

        assertEquals(42, dequeuedFirst.intValue());
        assertEquals(24, dequeuedSecond.intValue());

        dequeuedSecond.doFinalize();
        assertSumOfEntries(queue, 42 + 24);

        dequeuedFirst.doFinalize();
        assertQueueEmpty(queue);
    }

    @Test
    public void testEmptyAfterDequeueAndFinalize() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        fillQueue(queue);

        FinalizableInteger entry;
        while ((entry = queue.dequeue()) != null) {
            entry.doFinalize();
        }

        Consumer<FinalizableInteger> mockConsumer = (Consumer<FinalizableInteger>) mock(Consumer.class);
        queue.consumeAll(mockConsumer);
        verify(mockConsumer, never()).accept(any(FinalizableInteger.class));
    }

    @Test
    public void testTwiceEnqueueDequeue() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sum1 = fillQueue(queue);

        AtomicLong actual = new AtomicLong();
        FinalizableInteger entry;
        while ((entry = queue.dequeue()) != null) {
            actual.addAndGet(entry.intValue());
        }

        long sum2 = fillQueue(queue);
        while ((entry = queue.dequeue()) != null) {
            actual.addAndGet(entry.intValue());
        }

        assertEquals(sum1 + sum2, actual.get());
    }

    @Test
    public void testSize() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(1));
        queue.offer(of(2));
        queue.offer(of(3));

        assertEquals(3, queue.size());

        queue.poll();
        assertEquals(2, queue.size());

        queue.dequeue();
        assertEquals(1, queue.size());

        queue.dequeue();
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrain() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sum = fillQueue(queue);
        assertSumOfEntries(queue, sum);

        queue.clear();
        assertQueueEmpty(queue);
    }

    @Test
    public void testEnqueueAfterClear() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sum = fillQueue(queue);
        assertSumOfEntries(queue, sum);

        int clear = queue.clear();
        assertEquals(QUEUE_SIZE, clear);

        sum = fillQueue(queue);
        assertSumOfEntries(queue, sum);

        AtomicLong actual = new AtomicLong();
        FinalizableInteger entry;
        while ((entry = queue.dequeue()) != null) {
            actual.addAndGet(entry.intValue());
            entry.doFinalize();
        }
        assertEquals(sum, actual.get());

        assertQueueEmpty(queue);
    }

    @Test
    public void testClearOnEmptyQueueReturnsZero() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        int cleared = queue.clear();
        assertEquals(0, cleared);
    }

    @Test
    public void testEnqueueClearCycles() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();

        for (int i = 0; i < 1000; i++) {
            fillQueue(queue);
            queue.clear();
        }
        queue.clear();

        assertQueueEmpty(queue);
    }

    @Test
    public void testDrainTo() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        long sum = fillQueue(queue);

        List<FinalizableInteger> entryList = new LinkedList<>();
        int countDrained = queue.drainTo(entryList, HALF_QUEUE_SIZE);
        assertEquals(HALF_QUEUE_SIZE, countDrained);
        assertEquals(HALF_QUEUE_SIZE, queue.size());
        assertSumOfEntries(queue, sum);

        countDrained = queue.drainTo(entryList, HALF_QUEUE_SIZE);
        assertEquals(HALF_QUEUE_SIZE, countDrained);
        assertEquals(0, queue.size());
        assertSumOfEntries(queue, sum);

        countDrained = queue.drainTo(entryList, HALF_QUEUE_SIZE);
        assertEquals(0, countDrained);
        assertEquals(0, queue.size());
        assertSumOfEntries(queue, sum);
    }

    @Test
    public void testDrainToThrowsOnNull() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));

        assertThrows(NullPointerException.class, () -> queue.drainTo(null, 1));
        assertEquals(1, queue.size());
    }

    @Test
    public void testDrainToThrowsOnNegativeCount() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));

        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(new LinkedList<>(), -1));
        assertEquals(1, queue.size());
    }

    @Test
    public void testConsumeAllSingleConsumer() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));

        queue.consumeAll(v -> assertEquals(42, v.intValue()));
    }

    @Test
    public void testConsumeAllSingleConsumerThrowsOnNullConsumer() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();

        assertThrows(NullPointerException.class, () -> queue.consumeAll(null));
    }

    @Test
    public void testConsumeAllTwoConsumers() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        queue.offer(of(42));
        queue.offer(of(24));
        queue.dequeue();

        queue.consumeAll(
                v -> assertEquals(24, v.intValue()),
                v -> assertEquals(42, v.intValue()));
    }

    @Test
    public void testConsumeAllTwoConsumersThrowsOnNullConsumers() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();

        assertThrows(NullPointerException.class, () -> queue.consumeAll(null, v -> { }));
        assertThrows(NullPointerException.class, () -> queue.consumeAll(v -> { }, null));
    }

    @Test
    public void testFinalizeAfterClear() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        fillQueue(queue);

        List<FinalizableInteger> entries = new LinkedList<>();
        FinalizableInteger entry;
        while ((entry = queue.dequeue()) != null) {
            entries.add(entry);
        }

        queue.clear();
        assertQueueEmpty(queue);

        entries.forEach(FinalizableInteger::doFinalize);
    }

    @Test
    public void testClearReturnsCountNotDequeued() {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        fillQueue(queue);

        for (int i = 0; i < HALF_QUEUE_SIZE / 2; i++) {
            queue.dequeue();
        }

        int countCleared = queue.clear();

        assertEquals(HALF_QUEUE_SIZE + HALF_QUEUE_SIZE / 2, countCleared);
        assertQueueEmpty(queue);
    }

    private static long fillQueue(TwoPhasedLinkedQueue<FinalizableInteger> queue) {
        long expected = 0;
        for (int i = 0; i < QUEUE_SIZE; i++) {
            queue.offer(of(i));
            expected += i;
        }
        return expected;
    }

    private static void assertSumOfEntries(TwoPhasedLinkedQueue<FinalizableInteger> queue, long expectedSum) {
        AtomicLong actual = new AtomicLong();
        queue.consumeAll(e -> actual.addAndGet(e.intValue()));
        assertEquals(expectedSum, actual.get());
    }

    private static void assertQueueEmpty(TwoPhasedLinkedQueue<FinalizableInteger> queue) {
        assertSumOfEntries(queue, 0);
        assertEquals(0, queue.size());
    }
}
