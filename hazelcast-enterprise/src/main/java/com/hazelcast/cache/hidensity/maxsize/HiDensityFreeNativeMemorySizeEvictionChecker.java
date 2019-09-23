package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryUnit;

/**
 * Max-size policy implementation for {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#FREE_NATIVE_MEMORY_SIZE}.
 */
public class HiDensityFreeNativeMemorySizeEvictionChecker
        implements EvictionChecker {

    private final HazelcastMemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemorySizeEvictionChecker(HazelcastMemoryManager memoryManager, int size) {
        this.memoryManager = memoryManager;
        this.minFreeMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isEvictionRequired() {
        return memoryManager.getMemoryStats().getFreeNative() < minFreeMemorySize;
    }
}
