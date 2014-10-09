package com.hazelcast.elasticcollections.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @author mdogan 22/01/14
 */
public final class LinkedBlockingOffHeapQueue<E> extends AbstractBlockingOffHeapQueue<E> implements BlockingOffHeapQueue<E> {

    public LinkedBlockingOffHeapQueue(EnterpriseSerializationService ss) {
        super(ss, Integer.MAX_VALUE, null);
    }

    public LinkedBlockingOffHeapQueue(EnterpriseSerializationService ss, final int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongBlockingQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongLinkedBlockingQueue(malloc, capacity, nullItem);
    }
}
