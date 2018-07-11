package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HiDensityCacheClearTest extends CacheClearTest {

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

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
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

}
