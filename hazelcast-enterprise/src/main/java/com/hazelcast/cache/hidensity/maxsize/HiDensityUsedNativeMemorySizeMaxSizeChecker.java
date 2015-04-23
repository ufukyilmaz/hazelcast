package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
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
