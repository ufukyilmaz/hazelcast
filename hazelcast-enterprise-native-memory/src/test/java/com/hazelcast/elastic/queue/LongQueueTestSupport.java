package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class LongQueueTestSupport {

    static final int CAPACITY = 1000;
    static final long NULL = -1;

    private MemoryManager memoryManager;
    private final Random random = new Random();
    protected LongQueue queue;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        queue = createQueue(memoryManager);
    }

    protected abstract LongQueue createQueue(MemoryManager memoryManager);

    @After
    public void tearDown() throws Exception {
        queue.dispose();
        memoryManager.destroy();
    }

    @Test
    public void testOffer() throws Exception {
        assertTrue(queue.offer(newItem()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOffer_null() throws Exception {
        queue.offer(NULL);
    }

    @Test
    public void testFullCapacity() throws Exception {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(newItem());
        }
        assertFalse(queue.offer(newItem()));
    }

    @Test
    public void testPeek() throws Exception {
        assertEquals(NULL, queue.peek());
        long value = newItem();
        queue.offer(value);
        assertEquals(value, queue.peek());
        assertEquals(1, queue.size());
    }

    @Test
    public void testPoll() throws Exception {
        assertEquals(NULL, queue.poll());
        long value = newItem();
        queue.offer(value);
        assertEquals(value, queue.poll());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, queue.size());

        queue.offer(newItem());
        assertEquals(1, queue.size());

        for (int i = 0; i < 100; i++) {
            queue.offer(newItem());
        }
        assertEquals(101, queue.size());
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(queue.isEmpty());
        queue.offer(newItem());
        assertFalse(queue.isEmpty());
    }

    @Test
    public void testCapacity() throws Exception {
        assertEquals(CAPACITY, queue.capacity());
        assertEquals(CAPACITY, queue.remainingCapacity());

        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer(newItem());
        }
        assertEquals(CAPACITY, queue.capacity());
        assertEquals(CAPACITY - count, queue.remainingCapacity());

        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(newItem());
        }
        assertEquals(CAPACITY, queue.capacity());
        assertEquals(0, queue.remainingCapacity());
    }

    @Test
    public void testClear() throws Exception {
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer(newItem());
        }

        queue.clear();
        assertEquals(0, queue.size());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testNullItem() throws Exception {
        assertEquals(NULL, queue.nullItem());
    }

    @Test
    public void testOfferAndPoll() throws Exception {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(i, queue.poll());
        }
    }

    @Test
    public void testIterator() throws Exception {
        assertFalse(queue.iterator().hasNext());

        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        LongIterator iterator = queue.iterator();
        for (int i = 0; i < CAPACITY; i++) {
            assertTrue(iterator.hasNext());
            long next = iterator.next();
            assertEquals(i, next);
        }

        assertFalse(iterator.hasNext());
        iterator.reset();

        for (int i = 0; i < CAPACITY; i++) {
            assertTrue(iterator.hasNext());
            long next = iterator.next();
            assertEquals(i, next);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOffer_after_destroy() throws Exception {
        queue.dispose();
        queue.offer(newItem());
    }

    @Test(expected = IllegalStateException.class)
    public void testPeek_after_destroy() throws Exception {
        queue.dispose();
        queue.peek();
    }

    @Test(expected = IllegalStateException.class)
    public void testPoll_after_destroy() throws Exception {
        queue.dispose();
        queue.poll();
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_after_destroy() throws Exception {
        queue.dispose();
        queue.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_next_after_destroy() throws Exception {
        queue.offer(newItem());
        LongIterator iterator = queue.iterator();
        queue.dispose();
        iterator.next();
    }

    private long newItem() {
        long item;
        while ((item = random.nextLong()) == NULL);
        return item;
    }
}
