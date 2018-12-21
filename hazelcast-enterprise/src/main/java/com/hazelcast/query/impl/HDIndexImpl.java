package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

import static com.hazelcast.nio.serialization.DataType.HEAP;

/**
 * Provides implementation of off-heap indexes.
 */
public class HDIndexImpl extends AbstractIndex {

    private static final int UNINDEXED = -1;

    private int indexedPartition = UNINDEXED;

    public HDIndexImpl(String attributeName, boolean ordered, EnterpriseSerializationService ss, Extractors extractors,
                       PerIndexStats stats) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(attributeName, ordered, ss, extractors, IndexCopyBehavior.NEVER, stats);
    }

    @Override
    protected IndexStore createIndexStore(boolean ordered, PerIndexStats stats) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) ss;
        MemoryAllocator malloc = stats.wrapMemoryAllocator(ess.getCurrentMemoryAllocator());
        MapEntryFactory<QueryableEntry> entryFactory = new OnHeapEntryFactory(ess, extractors);
        return ordered ? new HDSortedIndexStore(ess, malloc, entryFactory) : new HDUnsortedIndexStore(ess, malloc, entryFactory);
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

    @Override
    public void destroy() {
        indexStore.destroy();
        super.destroy();
    }

    @Override
    public void clear() {
        super.clear();
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
