package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.hidensity.HiDensityRecordStore;
import com.hazelcast.memory.MemoryManager;

/**
 * @author sozal 20/11/14
 */
public class HiDensityFreeNativeMemoryPercentageMaxSizeChecker implements MaxSizeChecker {

    private final MemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemoryPercentageMaxSizeChecker(MemoryManager memoryManager,
                                                             int size, long maxNativeMemory) {
        this.memoryManager = memoryManager;
        final int maxSizePercentage = size;
        if (maxSizePercentage < 0 || maxSizePercentage > HiDensityRecordStore.HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        final double maxSizeRatio = (double) maxSizePercentage / (double) HiDensityRecordStore.HUNDRED_PERCENT;
        this.minFreeMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNativeMemory() < minFreeMemorySize;
    }

}
