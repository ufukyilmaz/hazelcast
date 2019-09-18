package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.DefaultOperationProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

/**
 * Provides enterprise cache operations.
 */
public class EnterpriseCacheOperationProvider extends DefaultOperationProvider {

    public EnterpriseCacheOperationProvider(String nameWithPrefix) {
        super(nameWithPrefix);
    }

    public Operation createWanRemoveOperation(String origin, Data key, int completionId) {
        return new WanCacheRemoveOperation(nameWithPrefix, origin, key, completionId);
    }

    public Operation createWanMergeOperation(CacheMergeTypes mergingEntry,
                                             SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy, int completionId) {
        return new WanCacheMergeOperation(nameWithPrefix, mergingEntry, mergePolicy, completionId);
    }
}
