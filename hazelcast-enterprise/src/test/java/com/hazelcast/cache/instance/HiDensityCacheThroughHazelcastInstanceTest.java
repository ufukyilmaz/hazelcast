package com.hazelcast.cache.instance;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        return super.createCacheSimpleConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected CacheConfig createCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        return super.createCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
