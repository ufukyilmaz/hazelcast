package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#ENTRY_COUNT}.
 */
public class HiDensityEntryCountMaxSizeChecker implements MaxSizeChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxEntryCount;

    public HiDensityEntryCountMaxSizeChecker(HiDensityStorageInfo storageInfo, int size) {
        this.storageInfo = storageInfo;
        this.maxEntryCount = size;
    }

    @Override
    public boolean isReachedToMaxSize() {
        return storageInfo.getEntryCount() >= maxEntryCount;
    }
}
