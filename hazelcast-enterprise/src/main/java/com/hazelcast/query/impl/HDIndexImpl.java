package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.query.impl.getters.Extractors;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * Provides implementation of off-heap indexes.
 */
public class HDIndexImpl extends AbstractIndex {

    private static final int UNINDEXED = -1;

    private int indexedPartition = UNINDEXED;

    public HDIndexImpl(
            IndexConfig config,
            EnterpriseSerializationService ss,
            Extractors extractors,
            PerIndexStats stats) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(config, ss, extractors, IndexCopyBehavior.NEVER, stats);
    }

    @Override
    protected IndexStore createIndexStore(IndexConfig config, PerIndexStats stats) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) ss;
        MemoryAllocator malloc = stats.wrapMemoryAllocator(ess.getCurrentMemoryAllocator());
        MapEntryFactory<QueryableEntry> entryFactory = new OnHeapEntryFactory(ess, extractors);

        switch (config.getType()) {
            case SORTED:
                return new HDOrderedIndexStore(ess, malloc, entryFactory);
            case HASH:
                return new HDUnorderedIndexStore(ess, malloc, entryFactory);
            case BITMAP:
                throw new IllegalArgumentException("Bitmap indexes are not supported by NATIVE storage");
            default:
                throw new IllegalArgumentException("unexpected index type: " + config.getType());
        }
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
    public boolean allPartitionsIndexed(int ownedPartitionCount) {
        return indexedPartition != UNINDEXED;
    }

    @Override
    public void beginPartitionUpdate() {
        // No-op.
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
    public long getPartitionStamp(PartitionIdSet expectedPartitionIds) {
        return GlobalIndexPartitionTracker.STAMP_INVALID;
    }

    @Override
    public boolean validatePartitionStamp(long stamp) {
        return false;
    }

    /**
     * Converts off-heap key-value pairs back to on-heap queryable entries.
     */
    static class OnHeapEntryFactory implements MapEntryFactory<QueryableEntry> {

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
