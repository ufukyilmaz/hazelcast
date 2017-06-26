package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.HDTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheLocalImmediateInvalidateTest extends NearCacheLocalImmediateInvalidateTest {

    @Override
    protected Config createConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(90);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        Config config = HDTestSupport.getHDConfig();

        MapConfig mapConfig = config.getMapConfig(MAP_NAME + "*");
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return config;
    }
}
