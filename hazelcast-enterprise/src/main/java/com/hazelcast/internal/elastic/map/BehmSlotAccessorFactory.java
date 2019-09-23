package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.memory.MemoryAllocator;

/**
 * A factory for creating {@link BehmSlotAccessor} instances.
 *
 * The reason we need this factory, is that in {@link BinaryElasticHashMap} we have to deal with different
 * types of BEHM accessors.
 */
public interface BehmSlotAccessorFactory {

    /**
     * Creates {@link BehmSlotAccessor}
     * @param malloc the memory allocator
     * @param baseAddr the base address
     * @param size the size of the memory
     * @return the newly created slot accessor
     */
    BehmSlotAccessor create(MemoryAllocator malloc, long baseAddr, long size);

    /**
     * Creates {@link BehmSlotAccessor} with the same memory allocator, base address and memory size as {@code that} has.
     * @param that the slot accessor
     * @return the newly created slot accessor
     */
    BehmSlotAccessor create(BehmSlotAccessor that);

}
