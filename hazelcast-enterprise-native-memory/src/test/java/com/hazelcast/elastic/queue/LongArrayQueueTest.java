package com.hazelcast.elastic.queue;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongArrayQueueTest extends LongQueueTestSupport {

    @Override
    protected LongQueue createQueue(MemoryManager memoryManager) {
        return new LongArrayQueue(memoryManager, CAPACITY, NULL);
    }
}
