package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.internal.elastic.tree.BinaryElasticNestedTreeMap;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.elastic.tree.OffHeapComparator;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.Map;
import java.util.Set;

/**
 * Nested map for HD index. See details ib {@link BinaryElasticNestedTreeMap}.
 * @param <T> type of the Map.Entry returned by the tree.
 */
public final class HDIndexBinaryElasticNestedTreeMap<T extends Map.Entry> extends BinaryElasticNestedTreeMap<T, MemoryBlock> {

    private final HDExpirableIndexStore indexStore;

    public HDIndexBinaryElasticNestedTreeMap(HDExpirableIndexStore indexStore, EnterpriseSerializationService ess,
                                             MemoryAllocator malloc, OffHeapComparator keyComparator,
                                             MapEntryFactory mapEntryFactory) {
        super(ess, malloc, keyComparator, mapEntryFactory, new HDIndexBehmSlotAccessorFactory(),
                new HDIndexBehmMemoryBlockAccessor(new NativeMemoryDataAccessor(ess), new HDRecordAccessor(ess)));
        this.indexStore = indexStore;
    }

    @Override
    protected void addEntries(Set<T> result, Set<Map.Entry<Data, MemoryBlock>> entrySet) {
        for (Map.Entry<Data, MemoryBlock> entry : entrySet) {
            MemoryBlock memoryBlock = entry.getValue();
            NativeMemoryData valueData;
            if (memoryBlock instanceof NativeMemoryData || memoryBlock == null) {
                valueData = (NativeMemoryData) memoryBlock;
            } else {
                valueData = indexStore.getValueOrNullIfExpired(memoryBlock);
            }
            if (memoryBlock == null || valueData != null) {
                result.add(mapEntryFactory.create(entry.getKey(), valueData));
            }
        }
    }
}
