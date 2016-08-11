package com.hazelcast.client.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HiDensityClientCacheClearTest
        extends ClientCacheClearTest {

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        memoryConfig
                .setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES));
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(10);
        cacheConfig.setEvictionConfig(evictionConfig);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        return cacheConfig;
    }
}
