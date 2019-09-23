package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @param <E> entry type
 */
public final class ArrayElasticQueue<E> extends AbstractElasticQueue<E> implements ElasticQueue<E> {

    public ArrayElasticQueue(EnterpriseSerializationService ss, int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongArrayQueue(malloc, capacity, nullItem);
    }
}
