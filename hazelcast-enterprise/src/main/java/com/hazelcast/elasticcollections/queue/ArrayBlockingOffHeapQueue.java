package com.hazelcast.elasticcollections.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @author mdogan 22/01/14
 */
public final class ArrayBlockingOffHeapQueue<E> extends AbstractBlockingOffHeapQueue<E> implements BlockingOffHeapQueue<E> {

    public ArrayBlockingOffHeapQueue(EnterpriseSerializationService ss, final int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongBlockingQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongArrayBlockingQueue(malloc, capacity, nullItem);
    }
}
