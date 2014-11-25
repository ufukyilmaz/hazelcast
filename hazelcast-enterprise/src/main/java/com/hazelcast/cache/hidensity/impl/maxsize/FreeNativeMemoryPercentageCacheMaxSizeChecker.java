package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.config.CacheMaxSizeConfig;
import com.hazelcast.memory.MemoryManager;

/**
 * @author sozal 20/11/14
 */
public class FreeNativeMemoryPercentageCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final MemoryManager memoryManager;
    private final long minFreeMemorySize;

    public FreeNativeMemoryPercentageCacheMaxSizeChecker(MemoryManager memoryManager,
            CacheMaxSizeConfig maxSizeConfig, long maxNativeMemory) {
        this.memoryManager = memoryManager;
        final int maxSizePercentage = maxSizeConfig.getSize();
        if (maxSizePercentage < 0 || maxSizePercentage > ICacheRecordStore.ONE_HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + CacheMaxSizeConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        final double maxSizeRatio = (double) maxSizePercentage / (double) ICacheRecordStore.ONE_HUNDRED_PERCENT;
        this.minFreeMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNativeMemory() < minFreeMemorySize;
    }

}
