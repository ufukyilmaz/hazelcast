package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

/**
 * @param <E> entry type
 */
public class LinkedElasticQueue<E> extends AbstractElasticQueue<E> implements ElasticQueue<E> {

    public LinkedElasticQueue(EnterpriseSerializationService ss) {
        this(ss, Integer.MAX_VALUE);
    }

    public LinkedElasticQueue(EnterpriseSerializationService ss, int capacity) {
        super(ss, capacity, null);
    }

    @Override
    protected LongQueue createQueue(final MemoryAllocator malloc, final int capacity, final long nullItem) {
        return new LongLinkedQueue(malloc, capacity, nullItem);
    }
}
