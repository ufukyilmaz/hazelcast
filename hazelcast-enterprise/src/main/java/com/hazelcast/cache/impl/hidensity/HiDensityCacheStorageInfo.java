package com.hazelcast.cache.impl.hidensity;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;

/**
 * Holds information about Hi-Density cache storage such as entry count, used memory, etc.
 */
public class HiDensityCacheStorageInfo extends HiDensityStorageInfo {

    private final CacheContext cacheContext;

    public HiDensityCacheStorageInfo(String cacheName, CacheContext cacheContext) {
        super(cacheName);
        this.cacheContext = cacheContext;
    }

    @Override
    public long addEntryCount(long count) {
        return cacheContext.increaseEntryCount(count);
    }

    @Override
    public long removeEntryCount(long count) {
        return cacheContext.decreaseEntryCount(count);
    }

    @Override
    public long increaseEntryCount() {
        return cacheContext.increaseEntryCount();
    }

    @Override
    public long decreaseEntryCount() {
        return cacheContext.decreaseEntryCount();
    }

    @Override
    public long getEntryCount() {
        return cacheContext.getEntryCount();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityCacheStorageInfo)) {
            return false;
        }
        return super.equals(obj);
    }
}
