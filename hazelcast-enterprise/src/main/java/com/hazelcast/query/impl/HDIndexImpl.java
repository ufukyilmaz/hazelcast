package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * Provides implementation of off-heap indexes.
 */
public class HDIndexImpl extends AbstractIndex {

    private static final int UNINDEXED = -1;

    private int indexedPartition = UNINDEXED;

    public HDIndexImpl(String name, String[] components, boolean ordered, EnterpriseSerializationService ss,
                       Extractors extractors, PerIndexStats stats, StoreAdapter partitionStoreAdapter) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(name, components, ordered, ss, extractors, IndexCopyBehavior.NEVER, stats, partitionStoreAdapter);
    }

    @Override
    protected IndexStore createIndexStore(boolean ordered, PerIndexStats stats) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) ss;
        MemoryAllocator malloc = stats.wrapMemoryAllocator(ess.getCurrentMemoryAllocator());
        MapEntryFactory<QueryableEntry> entryFactory = new OnHeapEntryFactory(ess, extractors);
        return ordered ? new HDOrderedIndexStore(ess, malloc, entryFactory, getPartitionStoreAdapter())
                : new HDUnorderedIndexStore(ess, malloc, entryFactory, getPartitionStoreAdapter());
    }

    @Override
    public void clear() {
        super.clear();
        indexedPartition = UNINDEXED;
    }

    @Override
    public void destroy() {
        indexStore.destroy();
        super.destroy();
    }

    @Override
    public boolean hasPartitionIndexed(int partitionId) {
        return indexedPartition == partitionId;
    }

    @Override
    public void markPartitionAsIndexed(int partitionId) {
        assert indexedPartition == UNINDEXED;
        indexedPartition = partitionId;
    }

    @Override
    public void markPartitionAsUnindexed(int partitionId) {
        indexedPartition = UNINDEXED;
    }

    /**
     * Converts off-heap key-value pairs back to on-heap queryable entries.
     */
    private static class OnHeapEntryFactory implements MapEntryFactory<QueryableEntry> {

        private final EnterpriseSerializationService ess;
        private final Extractors extractors;

        OnHeapEntryFactory(EnterpriseSerializationService ess, Extractors extractors) {
            this.ess = ess;
            this.extractors = extractors;
        }

        @Override
        public CachedQueryEntry create(Data key, Data value) {
            Data heapData = toHeapData(key);
            Data heapValue = toHeapData(value);
            return new CachedQueryEntry(ess, heapData, heapValue, extractors);
        }

        private Data toHeapData(Data data) {
            if (data instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
                if (nativeMemoryData.totalSize() == 0) {
                    return null;
                }
                return ess.toData(nativeMemoryData, HEAP);
            }
            return data;
        }

    }

}
