package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.hidensity.HiDensityStorageInfo;

/**
 * {@link HiDensityStorageInfo.EntryCountResolver} implementation for tracking cache entry counts.
 */
public class HiDensityCacheEntryCountResolver
        implements HiDensityStorageInfo.EntryCountResolver {

    private final CacheContext cacheContext;

    public HiDensityCacheEntryCountResolver(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    @Override
    public long getEntryCount() {
        return cacheContext.getEntryCount();
    }

    @Override
    public long increaseEntryCount() {
        return cacheContext.increaseEntryCount();
    }

    @Override
    public long increaseEntryCount(long count) {
        return cacheContext.increaseEntryCount(count);
    }

    @Override
    public long decreaseEntryCount() {
        return cacheContext.decreaseEntryCount();
    }

    @Override
    public long decreaseEntryCount(long count) {
        return cacheContext.decreaseEntryCount(count);
    }

}
