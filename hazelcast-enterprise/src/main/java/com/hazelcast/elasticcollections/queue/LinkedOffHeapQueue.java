package com.hazelcast.elasticcollections.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @author mdogan 22/01/14
 */
public class LinkedOffHeapQueue<E> extends AbstractOffHeapQueue<E> implements OffHeapQueue<E> {

    public LinkedOffHeapQueue(EnterpriseSerializationService ss) {
        this(ss, Integer.MAX_VALUE);
    }

    public LinkedOffHeapQueue(EnterpriseSerializationService ss, int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongLinkedQueue(malloc, capacity, nullItem);
    }
}
