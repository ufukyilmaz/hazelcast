package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.hidensity.HiDensityRecordStore;
import com.hazelcast.hidensity.HiDensityStorageInfo;

/**
 * @author sozal 20/11/14
 */
public class HiDensityUsedNativeMemoryPercentageMaxSizeChecker implements MaxSizeChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxUsedMemorySize;

    public HiDensityUsedNativeMemoryPercentageMaxSizeChecker(HiDensityStorageInfo storageInfo,
                                                             int size, long maxNativeMemory) {
        this.storageInfo = storageInfo;
        final int maxSizePercentage = size;
        if (maxSizePercentage < 0 || maxSizePercentage > HiDensityRecordStore.HUNDRED_PERCENT) {
            throw new IllegalArgumentException("\"maxSize\" can only be 0-100 for "
                    + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + " max-size policy !");
        }
        final double maxSizeRatio = (double) maxSizePercentage / (double) HiDensityRecordStore.HUNDRED_PERCENT;
        this.maxUsedMemorySize = (long) (maxNativeMemory * maxSizeRatio);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return storageInfo.getUsedMemory() >= maxUsedMemorySize;
    }

}
