package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;

/**
 * Holds information about Hi-Density cache storage such as entry count, used memory, etc.
 *
 * @author sozal 27/11/15
 */
public class HiDensityCacheStorageInfo extends HiDensityStorageInfo {

    private final CacheContext cacheContext;

    public HiDensityCacheStorageInfo(String cacheName, CacheContext cacheContext) {
        super(cacheName);
        this.cacheContext = cacheContext;
    }

    public long addEntryCount(long count) {
        return cacheContext.increaseEntryCount(count);
    }

    public long removeEntryCount(long count) {
        return cacheContext.decreaseEntryCount(count);
    }

    public long increaseEntryCount() {
        return cacheContext.increaseEntryCount();
    }

    public long decreaseEntryCount() {
        return cacheContext.decreaseEntryCount();
    }

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
