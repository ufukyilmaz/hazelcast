package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.elastic.map.NativeBehmSlotAccessorFactory;
import com.hazelcast.internal.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.util.Map;
import java.util.Set;

/**
 * Nested map of entries with the values represented as {@link NativeMemoryData}.
 * See more details in {@link BinaryElasticNestedTreeMap}.
 * @param <T> type of the Map.Entry returned by the tree.
 */
public final class NativeBinaryElasticNestedTreeMap<T extends Map.Entry> extends BinaryElasticNestedTreeMap<T, NativeMemoryData> {

    public NativeBinaryElasticNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc,
                                            OffHeapComparator keyComparator) {
        super(ess, malloc, keyComparator, new NativeBehmSlotAccessorFactory(), new NativeMemoryDataAccessor(ess));
    }


    @Override
    protected void addEntries(Set<T> result, Set<Map.Entry<Data, MemoryBlock>> entrySet) {
        for (Map.Entry<Data, MemoryBlock> mapEntry : entrySet) {
            result.add(mapEntryFactory.create(mapEntry.getKey(), (NativeMemoryData) mapEntry.getValue()));
        }
    }

}
