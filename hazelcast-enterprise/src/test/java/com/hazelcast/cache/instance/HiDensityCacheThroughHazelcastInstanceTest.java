package com.hazelcast.cache.instance;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HiDensityCacheThroughHazelcastInstanceTest extends CacheThroughHazelcastInstanceTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(MEMORY_SIZE);
        return config;
    }

    @Override
    protected CacheSimpleConfig createCacheSimpleConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        CacheSimpleConfig cacheSimpleConfig = super.createCacheSimpleConfig(cacheName);
        cacheSimpleConfig
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
        return cacheSimpleConfig;
    }

    @Override
    protected CacheConfig createCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        CacheConfig cacheConfig = super.createCacheConfig(cacheName);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
        return cacheConfig;
    }
}
