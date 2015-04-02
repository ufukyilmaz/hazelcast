package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.hidensity.HiDensityStorageInfo;

/**
 * @author sozal 20/11/14
 */
public class UsedNativeMemoryPercentageCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final HiDensityStorageInfo cacheInfo;
    private final long maxUsedMemorySize;

    public UsedNativeMemoryPercentageCacheMaxSizeChecker(HiDensityStorageInfo cacheInfo,
            int size, long maxNativeMemory) {
        this.cacheInfo = cacheInfo;
        final int maxSizePercentage = size;
        if (maxSizePercentage < 0 || maxSizePercentage > ICacheRecordStore.ONE_HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + CacheEvictionConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        final double maxSizeRatio = (double) maxSizePercentage / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT;
        this.maxUsedMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheInfo.getUsedMemory() >= maxUsedMemorySize;
    }

}
