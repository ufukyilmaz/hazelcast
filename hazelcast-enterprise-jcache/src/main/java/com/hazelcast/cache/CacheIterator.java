package com.hazelcast.cache;

import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;

/**
 * @author mdogan 15/05/14
 */
public class CacheIterator extends AbstractCacheIterator {

    private final NodeEngine nodeEngine;

    CacheIterator(ICache cache, NodeEngine nodeEngine, int batchCount) {
        super(cache, batchCount);
        this.nodeEngine = nodeEngine;
    }

    protected CacheIterationResult fetch() {
        CacheIterateOperation op = new CacheIterateOperation(cache.getName(), lastSlot, batchCount);
        InternalCompletableFuture<CacheIterationResult> f = nodeEngine.getOperationService()
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);

        return f.getSafely();
    }

    @Override
    protected int getPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    protected SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }
}
