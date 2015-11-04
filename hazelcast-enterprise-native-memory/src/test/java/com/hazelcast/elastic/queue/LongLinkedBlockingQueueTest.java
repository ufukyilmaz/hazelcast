package com.hazelcast.elastic.queue;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongLinkedBlockingQueueTest extends LongQueueTestSupport {

    @Override
    protected LongQueue createQueue(MemoryManager memoryManager) {
        return new LongLinkedBlockingQueue(memoryManager, CAPACITY, NULL);
    }

    @Override
    @Test
    public void testPeek_after_destroy() throws Exception {
        queue.dispose();
        assertEquals(NULL, queue.peek());
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testIterator() throws Exception {
        super.testIterator();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_after_destroy() throws Exception {
        super.testIterator_after_destroy();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_next_after_destroy() throws Exception {
        super.testIterator_next_after_destroy();
    }
}
