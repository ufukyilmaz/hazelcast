package com.hazelcast.cache.impl.hidensity.maxsize;

import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.internal.memory.HazelcastMemoryManager;

/**
 * Max-size policy implementation for {@link
 * MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE}.
 */
public class HiDensityFreeNativeMemoryPercentageEvictionChecker
        implements EvictionChecker {

    private final HazelcastMemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemoryPercentageEvictionChecker(HazelcastMemoryManager memoryManager,
                                                              int maxSizePercentage, long maxNativeMemory) {
        if (maxSizePercentage < 0 || maxSizePercentage > HiDensityRecordStore.HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        double maxSizeRatio = (double) maxSizePercentage / (double) HiDensityRecordStore.HUNDRED_PERCENT;

        this.memoryManager = memoryManager;
        this.minFreeMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isEvictionRequired() {
        return memoryManager.getMemoryStats().getFreeNative() < minFreeMemorySize;
    }
}
