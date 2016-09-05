package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.memory.HazelcastMemoryManager;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE}.
 */
public class HiDensityFreeNativeMemoryPercentageMaxSizeChecker implements MaxSizeChecker {

    private final HazelcastMemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemoryPercentageMaxSizeChecker(HazelcastMemoryManager memoryManager,
                                                             int maxSizePercentage, long maxNativeMemory) {
        if (maxSizePercentage < 0 || maxSizePercentage > HiDensityRecordStore.HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        double maxSizeRatio = (double) maxSizePercentage / (double) HiDensityRecordStore.HUNDRED_PERCENT;

        this.memoryManager = memoryManager;
        this.minFreeMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNative() < minFreeMemorySize;
    }
}
