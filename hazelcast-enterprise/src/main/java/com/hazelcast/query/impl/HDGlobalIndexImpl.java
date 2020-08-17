package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.GlobalIndexPoolingAllocator;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.query.impl.HDIndexImpl.OnHeapEntryFactory;
import static java.util.Collections.newSetFromMap;

/**
 * Provides implementation of global off-heap indexes.
 */

public class HDGlobalIndexImpl extends AbstractIndex {

    private final Set<Integer> indexedPartitions = newSetFromMap(new ConcurrentHashMap<>());


    public HDGlobalIndexImpl(
            IndexConfig config,
            EnterpriseSerializationService ss,
            Extractors extractors,
            PerIndexStats stats
    ) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(config, ss, extractors, IndexCopyBehavior.NEVER, stats, null);
    }

    @Override
    protected IndexStore createIndexStore(IndexConfig config, PerIndexStats stats) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) ss;
        HazelcastMemoryManager memoryManager = ess.getMemoryManager();
        if (!(memoryManager instanceof PoolingMemoryManager)) {
            throw new IllegalArgumentException("HD global indices require Pooling Memory Manager");
        }

        PoolingMemoryManager poolingMemoryManager = (PoolingMemoryManager) memoryManager;

        MemoryAllocator wrapedKeyAllocator = stats.wrapMemoryAllocator(poolingMemoryManager.getGlobalMemoryManager());
        GlobalIndexPoolingAllocator indexAllocator = poolingMemoryManager.getGlobalIndexAllocator();
        MemoryAllocator wrapedIndexAllocator = stats.wrapMemoryAllocator(indexAllocator);
        MapEntryFactory<QueryableEntry> entryFactory = new OnHeapEntryFactory(ess, extractors);
        int nodeSize = indexAllocator.getNodeSize();

        switch (config.getType()) {
            case SORTED:
                return new HDOrderedConcurrentIndexStore(copyBehavior,
                        ess, wrapedKeyAllocator, wrapedIndexAllocator, entryFactory, nodeSize);
            case HASH:
                return new HDUnorderedConcurrentIndexStore(copyBehavior,
                        ess, wrapedKeyAllocator, wrapedIndexAllocator, entryFactory, nodeSize);
            case BITMAP:
                throw new IllegalArgumentException("Bitmap indexes are not supported by NATIVE storage");
            default:
                throw new IllegalArgumentException("unexpected index type: " + config.getType());
        }
    }

    @Override
    public boolean hasPartitionIndexed(int partitionId) {
        return indexedPartitions.contains(partitionId);
    }

    @Override
    public boolean allPartitionsIndexed(int ownedPartitionCount) {
        // This check guarantees that all partitions are indexed
        // only if there is no concurrent migrations. Check migration stamp
        // to detect concurrent migrations if needed.
        return ownedPartitionCount < 0 || indexedPartitions.size() == ownedPartitionCount;
    }

    @Override
    public void markPartitionAsIndexed(int partitionId) {
        assert !indexedPartitions.contains(partitionId);
        indexedPartitions.add(partitionId);
    }

    @Override
    public void markPartitionAsUnindexed(int partitionId) {
        indexedPartitions.remove(partitionId);
    }

    @Override
    public void clear() {
        super.clear();
        indexedPartitions.clear();
    }

    @Override
    public void destroy() {
        indexStore.destroy();
        super.destroy();
    }

}
