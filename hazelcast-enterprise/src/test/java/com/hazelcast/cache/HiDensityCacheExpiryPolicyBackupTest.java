package com.hazelcast.cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityCacheExpiryPolicyBackupTest extends CacheExpiryPolicyBackupTest {

    @Override
    public Config getConfig() {
        Config cfg = super.getConfig();
        cfg.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(128, MemoryUnit.MEGABYTES));
        return cfg;
    }

    @Override
    protected CacheSimpleConfig getCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        CacheSimpleConfig cacheConfig = super.getCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }
}
