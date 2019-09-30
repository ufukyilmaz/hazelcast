package com.hazelcast.cache.impl.hidensity.maxsize;

import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#ENTRY_COUNT}.
 */
public class HiDensityEntryCountEvictionChecker
        implements EvictionChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxEntryCount;

    public HiDensityEntryCountEvictionChecker(HiDensityStorageInfo storageInfo, int size) {
        this.storageInfo = storageInfo;
        this.maxEntryCount = size;
    }

    @Override
    public boolean isEvictionRequired() {
        return storageInfo.getEntryCount() >= maxEntryCount;
    }
}
