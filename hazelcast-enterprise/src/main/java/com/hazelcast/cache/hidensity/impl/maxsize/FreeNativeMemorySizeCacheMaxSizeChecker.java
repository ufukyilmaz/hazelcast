package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class FreeNativeMemorySizeCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final MemoryManager memoryManager;
    private final long minFreeMemorySize;

    public FreeNativeMemorySizeCacheMaxSizeChecker(MemoryManager memoryManager, int size) {
        this.memoryManager = memoryManager;
        this.minFreeMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNativeMemory() < minFreeMemorySize;
    }

}
