package com.hazelcast.elastic.queue;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * @param <E> entry type.
 * @author mdogan 22/01/14
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
