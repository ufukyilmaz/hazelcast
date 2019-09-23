package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongArrayBlockingQueueTest extends LongBlockingQueueTestSupport {

    @Override
    protected LongQueue createQueue(HazelcastMemoryManager memoryManager) {
        return new LongArrayBlockingQueue(memoryManager, CAPACITY, NULL);
    }

}
