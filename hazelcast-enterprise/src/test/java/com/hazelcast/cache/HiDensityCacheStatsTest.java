package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HiDensityCacheStatsTest extends CacheStatsTest {

    private static String elasticMemoryEnabledPropertyValue;

    @BeforeClass
    public static void beforeAll() {
        elasticMemoryEnabledPropertyValue = System.getProperty("hazelcast.elastic.memory.enabled");
        System.setProperty("hazelcast.elastic.memory.enabled", "false");
    }

    @AfterClass
    public static void afterAll() {
        if (elasticMemoryEnabledPropertyValue != null) {
            System.setProperty("hazelcast.elastic.memory.enabled", elasticMemoryEnabledPropertyValue);
        }
    }

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
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(99);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

}
