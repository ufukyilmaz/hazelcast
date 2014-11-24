package com.hazelcast.cache.hidensity.impl.maxsize;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.config.CacheMaxSizeConfig;
import com.hazelcast.memory.MemoryUnit;

/**
 * @author sozal 20/11/14
 */
public class UsedNativeMemorySizeCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final HiDensityCacheInfo cacheInfo;
    private final long maxSize;

    public UsedNativeMemorySizeCacheMaxSizeChecker(HiDensityCacheInfo cacheInfo,
            CacheMaxSizeConfig maxSizeConfig) {
        this.cacheInfo = cacheInfo;
        this.maxSize = MemoryUnit.BYTES.convert(maxSizeConfig.getSize(), MemoryUnit.MEGABYTES);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheInfo.getUsedMemory() >= maxSize;
    }

}
