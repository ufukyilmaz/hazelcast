package com.hazelcast.elasticcollections.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @author mdogan 22/01/14
 */
public final class ArrayOffHeapQueue<E> extends AbstractOffHeapQueue<E> implements OffHeapQueue<E> {

    public ArrayOffHeapQueue(EnterpriseSerializationService ss, int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongArrayQueue(malloc, capacity, nullItem);
    }
}
