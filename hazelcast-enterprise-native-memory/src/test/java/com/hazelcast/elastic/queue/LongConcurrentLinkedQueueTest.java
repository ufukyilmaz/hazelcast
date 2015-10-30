package com.hazelcast.elastic.queue;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongConcurrentLinkedQueueTest extends LongQueueTestSupport {

    @Override
    protected LongQueue createQueue(MemoryManager memoryManager) {
        return new LongConcurrentLinkedQueue(memoryManager, NULL);
    }

    @Override
    @Test
    public void testCapacity() throws Exception {
        assertEquals(Integer.MAX_VALUE, queue.capacity());
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
    }

    @Override
    @Test
    @Ignore("not supported")
    public void testFullCapacity() throws Exception {
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testPeek() throws Exception {
        super.testPeek();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testPeek_after_destroy() throws Exception {
        super.testPeek_after_destroy();
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
