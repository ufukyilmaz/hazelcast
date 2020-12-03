package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.internal.elastic.tree.BinaryElasticNestedTreeMap;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.elastic.tree.OffHeapComparator;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.Map;
import java.util.Set;

/**
 * Nested map for HD index. See details ib {@link BinaryElasticNestedTreeMap}.
 *
 * @param <T> type of the Map.Entry returned by the tree.
 */
public final class HDIndexBinaryElasticNestedTreeMap<T extends Map.Entry>
        extends BinaryElasticNestedTreeMap<T, MemoryBlock> {

    public HDIndexBinaryElasticNestedTreeMap(EnterpriseSerializationService ess,
                                             MemoryAllocator malloc, OffHeapComparator keyComparator,
                                             MapEntryFactory mapEntryFactory) {
        super(ess, malloc, keyComparator, mapEntryFactory, new HDIndexBehmSlotAccessorFactory(),
                new HDIndexBehmMemoryBlockAccessor(new NativeMemoryDataAccessor(ess)));
    }

    @Override
    protected void addEntries(Set<T> result, Set<Map.Entry<Data, MemoryBlock>> entrySet) {
        for (Map.Entry<Data, MemoryBlock> entry : entrySet) {
            result.add(mapEntryFactory.create(entry.getKey(), ((NativeMemoryData) entry.getValue())));
        }
    }
}
