package com.hazelcast.client.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HiDensityClientCacheClearTest extends ClientCacheClearTest {

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");
        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES));

        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(10);

        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);

        return cacheConfig;
    }

    @Override
    protected void onSetup() {
        RuntimeAvailableProcessors.override(4);
        super.onSetup();
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        RuntimeAvailableProcessors.resetOverride();
    }
}
