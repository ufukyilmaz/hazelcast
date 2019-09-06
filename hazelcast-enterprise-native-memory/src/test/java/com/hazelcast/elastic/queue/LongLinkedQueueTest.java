package com.hazelcast.elastic.queue;

import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongLinkedQueueTest extends LongQueueTestSupport {

    @Override
    protected LongQueue createQueue(HazelcastMemoryManager memoryManager) {
        return new LongLinkedQueue(memoryManager, CAPACITY, NULL);
    }

}