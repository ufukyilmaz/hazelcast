package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.config.CacheMaxSizeConfig;

/**
 * @author sozal 20/11/14
 */
public class UsedNativeMemoryPercentageCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final HiDensityCacheInfo cacheInfo;
    private final long maxUsedMemorySize;

    public UsedNativeMemoryPercentageCacheMaxSizeChecker(HiDensityCacheInfo cacheInfo,
            CacheMaxSizeConfig maxSizeConfig, long maxNativeMemory) {
        this.cacheInfo = cacheInfo;
        final int maxSizePercentage = maxSizeConfig.getSize();
        if (maxSizePercentage < 0 || maxSizePercentage > ICacheRecordStore.ONE_HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + CacheMaxSizeConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        final double maxSizeRatio = (double) maxSizePercentage / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT;
        this.maxUsedMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheInfo.getUsedMemory() >= maxUsedMemorySize;
    }

}
