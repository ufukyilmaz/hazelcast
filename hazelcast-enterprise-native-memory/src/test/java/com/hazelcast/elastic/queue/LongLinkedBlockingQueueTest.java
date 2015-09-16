/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        queue.destroy();
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
