package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

/**
 * @param <E> entry type
 */
public final class ArrayBlockingElasticQueue<E> extends AbstractBlockingElasticQueue<E>
        implements BlockingElasticQueue<E> {

    public ArrayBlockingElasticQueue(EnterpriseSerializationService ss, final int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongBlockingQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongArrayBlockingQueue(malloc, capacity, nullItem);
    }
}
