package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

/**
 * @param <E> entry type
 */
public final class LinkedBlockingElasticQueue<E> extends AbstractBlockingElasticQueue<E>
        implements BlockingElasticQueue<E> {

    public LinkedBlockingElasticQueue(EnterpriseSerializationService ss) {
        super(ss, Integer.MAX_VALUE, null);
    }

    public LinkedBlockingElasticQueue(EnterpriseSerializationService ss, final int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongBlockingQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongLinkedBlockingQueue(malloc, capacity, nullItem);
    }
}
