package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class HiDensityFreeNativeMemorySizeMaxSizeChecker implements MaxSizeChecker {

    private final HazelcastMemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemorySizeMaxSizeChecker(HazelcastMemoryManager memoryManager, int size) {
        this.memoryManager = memoryManager;
        this.minFreeMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNative() < minFreeMemorySize;
    }

}
