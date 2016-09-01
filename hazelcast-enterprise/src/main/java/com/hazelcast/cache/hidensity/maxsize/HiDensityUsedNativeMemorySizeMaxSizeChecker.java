package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.MemoryUnit;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#USED_NATIVE_MEMORY_SIZE}.
 */
public class HiDensityUsedNativeMemorySizeMaxSizeChecker implements MaxSizeChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxUsedMemorySize;

    public HiDensityUsedNativeMemorySizeMaxSizeChecker(HiDensityStorageInfo storageInfo, int size) {
        this.storageInfo = storageInfo;
        this.maxUsedMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return storageInfo.getUsedMemory() >= maxUsedMemorySize;
    }
}
