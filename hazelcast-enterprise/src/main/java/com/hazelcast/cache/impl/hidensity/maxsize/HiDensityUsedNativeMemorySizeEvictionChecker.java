package com.hazelcast.cache.impl.hidensity.maxsize;

import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.MemoryUnit;

/**
 * Max-size policy implementation for {@link
 * MaxSizePolicy#USED_NATIVE_MEMORY_SIZE}.
 */
public class HiDensityUsedNativeMemorySizeEvictionChecker
        implements EvictionChecker {

    private final HiDensityStorageInfo storageInfo;
    private final long maxUsedMemorySize;

    public HiDensityUsedNativeMemorySizeEvictionChecker(HiDensityStorageInfo storageInfo, int size) {
        this.storageInfo = storageInfo;
        this.maxUsedMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isEvictionRequired() {
        return storageInfo.getUsedMemory() >= maxUsedMemorySize;
    }
}
