package com.hazelcast.cache.operation;

import com.hazelcast.cache.impl.DefaultOperationProvider;
import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * Provides enterprise cache operations
 */
public class EnterpriseCacheOperationProvider extends DefaultOperationProvider {

    public EnterpriseCacheOperationProvider(String nameWithPrefix) {
        super(nameWithPrefix);
    }

    public Operation createWanRemoveOperation(String origin, Data key, int completionId) {
        return new WanCacheRemoveOperation(nameWithPrefix, origin, key, completionId);
    }

    public Operation createWanMergeOperation(String origin, CacheEntryView<Data, Data> cacheEntryView,
                                             CacheMergePolicy mergePolicy, int completionId) {
        return new WanCacheMergeOperation(nameWithPrefix, origin, cacheEntryView, mergePolicy, completionId);
    }
}
