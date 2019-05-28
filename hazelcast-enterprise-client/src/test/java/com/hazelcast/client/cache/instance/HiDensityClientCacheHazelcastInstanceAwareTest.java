package com.hazelcast.client.cache.instance;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
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
public class HiDensityClientCacheHazelcastInstanceAwareTest extends ClientCacheHazelcastInstanceAwareTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(MEMORY_SIZE)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED);
        return config;
    }

    @Override
    protected CacheConfig<Integer, Integer> createCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(99);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        return super.createCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
