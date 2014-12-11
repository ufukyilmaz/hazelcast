package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class UsedNativeMemorySizeCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final HiDensityCacheInfo cacheInfo;
    private final long maxUsedMemorySize;

    public UsedNativeMemorySizeCacheMaxSizeChecker(HiDensityCacheInfo cacheInfo, int size) {
        this.cacheInfo = cacheInfo;
        this.maxUsedMemorySize = MemoryUnit.BYTES.convert(size, MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheInfo.getUsedMemory() >= maxUsedMemorySize;
    }

}
