package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.HDTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HDNearCacheStaleReadTest extends NearCacheStaleReadTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }

    @Override
    protected NearCacheConfig getNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(90);

        return nearCacheConfig;
    }
}
