package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.BehmSlotAccessor;
import com.hazelcast.internal.elastic.map.BehmSlotAccessorFactory;
import com.hazelcast.internal.memory.MemoryAllocator;

/**
 *  A factory for creating {@link BehmSlotAccessor} for HD index.
 */
public class HDIndexBehmSlotAccessorFactory implements BehmSlotAccessorFactory {

    @Override
    public BehmSlotAccessor create(MemoryAllocator malloc, long baseAddr, long size) {
        return new HDIndexBehmSlotAccessor(malloc, baseAddr, size);
    }

    @Override
    public BehmSlotAccessor create(BehmSlotAccessor that) {
        return new HDIndexBehmSlotAccessor(that);
    }
}
