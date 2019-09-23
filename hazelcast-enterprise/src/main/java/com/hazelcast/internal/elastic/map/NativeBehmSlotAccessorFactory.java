package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 *  A factory for creating {@link BehmSlotAccessor} instances. The addressed values are represented by
 *  {@link NativeMemoryData}.
 */
public class NativeBehmSlotAccessorFactory implements BehmSlotAccessorFactory {

    @Override
    public BehmSlotAccessor create(MemoryAllocator malloc, long baseAddr, long size) {
        return new BehmSlotAccessor(malloc, baseAddr, size);
    }

    @Override
    public BehmSlotAccessor create(BehmSlotAccessor that) {
        return new BehmSlotAccessor(that);
    }
}
