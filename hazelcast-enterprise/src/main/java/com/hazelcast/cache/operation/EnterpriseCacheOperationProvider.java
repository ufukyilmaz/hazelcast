package com.hazelcast.cache.operation;

import com.hazelcast.cache.impl.DefaultOperationProvider;
import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.cache.wan.CacheEntryView;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * Provides enterprise cache operations
 */
public class EnterpriseCacheOperationProvider extends DefaultOperationProvider {

    public EnterpriseCacheOperationProvider(String nameWithPrefix) {
        super(nameWithPrefix);
    }

    public Operation createWanRemoveOperation(String origin, Data key, Data value, int completionId) {
        return new WanCacheRemoveOperation(nameWithPrefix, origin, key, value, completionId);
    }

    public Operation createWanMergeOperation(String origin, CacheEntryView<Data, Data> cacheEntryView,
                                             CacheMergePolicy mergePolicy, int completionId) {
        return new WanCacheMergeOperation(nameWithPrefix, origin, cacheEntryView, mergePolicy, completionId);
    }
}
