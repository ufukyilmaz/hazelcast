package com.hazelcast.cache.hidensity.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class HiDensityFreeNativeMemorySizeMaxSizeChecker implements MaxSizeChecker {

    private final JvmMemoryManager memoryManager;
    private final long minFreeMemorySize;

    public HiDensityFreeNativeMemorySizeMaxSizeChecker(JvmMemoryManager memoryManager, int size) {
        this.memoryManager = memoryManager;
        this.minFreeMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getNativeMemoryStats().getFree() < minFreeMemorySize;
    }

}
