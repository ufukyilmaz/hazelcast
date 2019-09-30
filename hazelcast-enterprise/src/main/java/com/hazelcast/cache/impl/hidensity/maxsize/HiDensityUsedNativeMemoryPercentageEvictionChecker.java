package com.hazelcast.cache.impl.hidensity.maxsize;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#USED_NATIVE_MEMORY_PERCENTAGE}.
 */
public class HiDensityUsedNativeMemoryPercentageEvictionChecker
        implements EvictionChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxUsedMemorySize;

    public HiDensityUsedNativeMemoryPercentageEvictionChecker(HiDensityStorageInfo storageInfo,
                                                              int maxSizePercentage, long maxNativeMemory) {
        if (maxSizePercentage < 0 || maxSizePercentage > HiDensityRecordStore.HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        double maxSizeRatio = (double) maxSizePercentage / (double) HiDensityRecordStore.HUNDRED_PERCENT;

        this.storageInfo = storageInfo;
        this.maxUsedMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isEvictionRequired() {
        return storageInfo.getUsedMemory() >= maxUsedMemorySize;
    }
}
