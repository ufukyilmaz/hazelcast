package com.hazelcast.internal.elastic.queue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class LongBlockingQueueTestSupport extends LongQueueTestSupport {

    @Test
    public void testPutAndPoll() throws Exception {
        for (int i = 0; i < CAPACITY; i++) {
            ((LongBlockingQueue) queue).put(i);
        }

        assertEquals(CAPACITY, queue.size());

        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(i, queue.poll());
        }

        assertEquals(0, queue.size());
    }

    @Test
    public void testPutAndTake() throws Exception {
        for (int i = 0; i < CAPACITY; i++) {
            ((LongBlockingQueue) queue).put(i);
        }

        assertEquals(CAPACITY, queue.size());

        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(i, ((LongBlockingQueue) queue).take());
        }

        assertEquals(0, queue.size());
    }

}
