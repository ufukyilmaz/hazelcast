package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.config.CacheMaxSizeConfig;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class FreeNativeMemorySizeCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final MemoryManager memoryManager;
    private final long maxSize;

    public FreeNativeMemorySizeCacheMaxSizeChecker(MemoryManager memoryManager,
            CacheMaxSizeConfig maxSizeConfig) {
        this.memoryManager = memoryManager;
        this.maxSize = MemoryUnit.BYTES.convert(maxSizeConfig.getSize(), MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return memoryManager.getMemoryStats().getFreeNativeMemory() >= maxSize;
    }

}
